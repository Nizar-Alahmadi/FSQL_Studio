"""
FSQL Studio (2025-10-31)
"""

import os, re, sys, csv, json, time, shutil, traceback, webbrowser
from datetime import datetime
from pathlib import Path

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import duckdb
import pandas as pd
from pandas import DataFrame
try:
    import openpyxl
except Exception:
    openpyxl = None

APP_TITLE = "FSQL Studio"
CONFIG_DIR = Path.home() / ".fsql_studio"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
SETTINGS_PATH = CONFIG_DIR / "settings.json"
RECENTS_PATH  = CONFIG_DIR / "recent_servers.json"

SUPPORTED_EXTS = {".csv", ".tsv", ".txt", ".xlsx", ".xls"}


class _ToolTip:
    def __init__(self, widget, text, delay=400, wrap=380):
        self.widget = widget
        self.text = text or ""
        self.delay = delay
        self.wrap = wrap
        self._after_id = None
        self._tip = None
        widget.bind("<Enter>", self._schedule, add=True)
        widget.bind("<Leave>", self._hide, add=True)
        widget.bind("<ButtonPress>", self._hide, add=True)
        widget.bind("<Destroy>", self._hide, add=True)

    def _schedule(self, _=None):
        self._unschedule()
        self._after_id = self.widget.after(self.delay, self._show)

    def _unschedule(self):
        if self._after_id:
            try:
                self.widget.after_cancel(self._after_id)
            except Exception:
                pass
            self._after_id = None

    def _show(self):
        if self._tip or not self.text:
            return
        x = self.widget.winfo_rootx() + 10
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 8
        self._tip = tw = tk.Toplevel(self.widget)
        try:
            tw.wm_overrideredirect(True)
            tw.wm_geometry(f"+{x}+{y}")
            lbl = tk.Label(
                tw, text=self.text, justify="left",
                background="#ffffe0", foreground="#000",
                relief="solid", borderwidth=1, padx=6, pady=4, wraplength=self.wrap
            )
            lbl.pack()
        except Exception:
            try:
                tw.destroy()
            except Exception:
                pass
            self._tip = None

    def _hide(self, _=None):
        self._unschedule()
        if self._tip:
            try:
                self._tip.destroy()
            except Exception:
                pass
            self._tip = None


def _load_json(p: Path, default):
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _save_json(p: Path, data):
    try:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def load_settings():  return _load_json(SETTINGS_PATH, {})
def save_settings(d): _save_json(SETTINGS_PATH, d)
def load_recents():   return _load_json(RECENTS_PATH, [])
def save_recents(r):  _save_json(RECENTS_PATH, r[:8])

def add_recent(path: Path):
    rec = load_recents()
    s = str(path)
    if s in rec:
        rec.remove(s)
    rec.insert(0, s)
    save_recents(rec)

def sniff_delimiter(path: Path, default: str = ",") -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
            sample = f.read(64 * 1024)
        if not sample:
            return "\t" if path.suffix.lower() == ".tsv" else default
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|", "^"])
        return dialect.delimiter or default
    except Exception:
        return "\t" if path.suffix.lower() == ".tsv" else default

def sniff_encoding(path: Path) -> str:
    """Attempts to detect encoding from BOM; default is UTF-8."""
    try:
        with open(path, "rb") as f:
            b = f.read(4)
        if b.startswith(b"\xff\xfe\x00\x00"):
            return "utf-32-le"
        if b.startswith(b"\x00\x00\xfe\xff"):
            return "utf-32-be"
        if b.startswith(b"\xff\xfe"):
            return "utf-16-le"
        if b.startswith(b"\xfe\xff"):
            return "utf-16-be"
        if b.startswith(b"\xef\xbb\xbf"):
            return "utf-8-sig"
    except Exception:
        pass
    return "utf-8"

def has_utf8_bom(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(3) == b"\xef\xbb\xbf"
    except Exception:
        return False

def _esc_ident(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'

def _to_safe_schema(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", name).strip("_") or "root"
    if s[0].isdigit():
        s = "_" + s
    return s

class FileKind:
    CSV="csv"; TXT="txt"; EXCEL="excel"

def iter_tables_in_path(db_path: Path):
    """Yield (display_name, file_path, kind, sheet) without duplicates."""
    seen = set()
    for p in sorted(db_path.iterdir()):
        if not p.is_file(): continue
        ext = p.suffix.lower()
        if ext not in SUPPORTED_EXTS: continue
        if ext in {".xlsx", ".xls"}:
            try:
                xls = pd.ExcelFile(p)
                for sheet in xls.sheet_names:
                    disp = f"{p.stem}__{sheet}"
                    if disp in seen: continue
                    seen.add(disp); yield (disp, p, FileKind.EXCEL, sheet)
            except Exception:
                disp = f"{p.stem}__sheet1"
                if disp not in seen:
                    seen.add(disp); yield (disp, p, FileKind.EXCEL, None)
        else:
            disp = p.stem
            if disp in seen: continue
            seen.add(disp)
            yield (disp, p, FileKind.CSV if ext in {".csv",".tsv"} else FileKind.TXT, None)

class RegMeta:
    __slots__=("path","kind","sheet","delimiter","encoding")
    def __init__(self, path, kind, sheet, delimiter, encoding):
        self.path, self.kind, self.sheet, self.delimiter, self.encoding = path, kind, sheet, delimiter, encoding

class NameResolver:
    """Keeps a user-facing display name and generates a safe internal name for DuckDB."""
    def __init__(self):
        self.disp2int = {}
        self.int2disp = {}
        self.used = set()

    def _unique(self, schema, base):
        name, i = base, 2
        while (schema, name) in self.used:
            name = f"{base}_{i}"; i+=1
        self.used.add((schema,name)); return name

    def register(self, schema, display_name):
        base = re.sub(r"[^0-9a-zA-Z_]+", "_", display_name).strip("_") or "tbl"
        if base[0].isdigit(): base = "_" + base
        internal = self._unique(schema, base)
        self.disp2int[(schema,display_name)] = internal
        self.int2disp[(schema,internal)] = display_name
        return internal

    def to_internal(self, schema, display_name):
        return self.disp2int.get((schema,display_name))

    def to_display(self, schema, internal):
        return self.int2disp.get((schema,internal))

    def rewrite_sql(self, sql: str, schema_tables: dict[str, list[str]]):
        """
        Replaces schema.display with schema.internal, supporting:
        - "quotes" or [brackets]
        - Optional spaces around the dot
        - Case-insensitive matching
        """
        out = sql
        for sch, disp_list in schema_tables.items():
            for disp in sorted(disp_list, key=len, reverse=True):
                internal = self.to_internal(sch, disp)
                if not internal:
                    continue
                sch_pat = rf'(?:{re.escape(sch)}|"{re.escape(sch)}"|\[{re.escape(sch)}\])'
                disp_pat = rf'(?:{re.escape(disp)}|"{re.escape(disp)}"|\[{re.escape(disp)}\])'
                pat = rf'(?i)(?<![\w]){sch_pat}\s*\.\s*{disp_pat}(?![\w])'
                out = re.sub(pat, f"{sch}.{internal}", out)
        return out


class DuckCatalog:
    def __init__(self):
        self.con = duckdb.connect(database=":memory:")
        self.registry = {}
        self.schemas  = {}
        self.names    = NameResolver()
        self._excel_loaded = False
        self._re_dml   = re.compile(r"^\s*(INSERT|UPDATE|DELETE)\b", re.I|re.S)
        self._re_ctas  = re.compile(r"(?is)^\s*create\s+table\s+([A-Za-z_]\w*)\.([A-Za-z_]\w*)\s+as\s+(select\b.+)$")
        self._re_target= re.compile(r"(?i)\b(?:INTO|UPDATE|FROM)\s+([A-Za-z_]\w*)\.([A-Za-z_]\w*)")

    def drop_schema(self, schema: str):
        schema = _to_safe_schema(schema)
        try:
            self.con.execute(f"DROP SCHEMA IF EXISTS {_esc_ident(schema)} CASCADE;")
        except Exception:
            pass
        self.registry = {k: v for k, v in self.registry.items() if k[0] != schema}
        self.schemas.pop(schema, None)
        self.names.used = {pair for pair in self.names.used if pair[0] != schema}
        for k in list(self.names.disp2int.keys()):
            if k[0] == schema:
                del self.names.disp2int[k]
        for k in list(self.names.int2disp.keys()):
            if k[0] == schema:
                del self.names.int2disp[k]

    def reset(self):
        try: self.con.close()
        except Exception: pass
        self.__init__()

    def _ensure_excel(self):
        if self._excel_loaded: return
        try: self.con.execute("INSTALL excel;")
        except Exception: pass
        try:
            self.con.execute("LOAD excel;")
            self._excel_loaded = True
        except Exception:
            self._excel_loaded = False

    def attach_folder(self, schema: str, folder: Path):
        schema = _to_safe_schema(schema)
        self.schemas[schema] = folder
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {_esc_ident(schema)};")
    
        for display, fpath, kind, sheet in iter_tables_in_path(folder):
            internal = self.names.register(schema, display)
            try:
                if kind in (FileKind.CSV, FileKind.TXT):
                    enc = sniff_encoding(fpath)
                    try:
                        self.con.execute(
                            f"""
                            CREATE OR REPLACE VIEW {_esc_ident(schema)}.{_esc_ident(internal)} AS
                            SELECT * FROM read_csv_auto('{fpath.as_posix()}',
                                HEADER=TRUE, SAMPLE_SIZE=-1, ALL_VARCHAR=TRUE
                            );
                            """
                        )
                        self.registry[(schema, internal)] = RegMeta(fpath, kind, None, None, enc)
                    except Exception:
                        try:
                            delim = sniff_delimiter(fpath)
                            self.con.execute(
                                f"""
                                CREATE OR REPLACE VIEW {_esc_ident(schema)}.{_esc_ident(internal)} AS
                                SELECT * FROM read_csv('{fpath.as_posix()}',
                                    AUTO_DETECT=TRUE,
                                    HEADER=TRUE,
                                    DELIM='{delim}',
                                    QUOTE='"',
                                    ESCAPE='"',
                                    IGNORE_ERRORS=TRUE,
                                    NULL_PADDING=TRUE,
                                    SAMPLE_SIZE=-1,
                                    ALL_VARCHAR=TRUE,
                                    MAX_LINE_SIZE=10000000
                                );
                                """
                            )
                            self.registry[(schema, internal)] = RegMeta(fpath, kind, None, delim, enc)
                        except Exception:
                            try:
                                df = pd.read_csv(
                                    fpath, sep=None, engine="python",
                                    encoding=enc, on_bad_lines="skip"
                                )
                            except Exception:
                                df = pd.read_csv(
                                    fpath, sep=None, engine="python",
                                    encoding="utf-16", on_bad_lines="skip"
                                )
    
                            tmp = f"tmp_{schema}_{internal}"
                            self.con.register(tmp, df)
                            self.con.execute(
                                f"""
                                CREATE OR REPLACE TABLE {_esc_ident(schema)}.{_esc_ident(internal)} AS
                                SELECT * FROM {_esc_ident(tmp)};
                                """
                            )
                            self.con.unregister(tmp)
                            self.registry[(schema, internal)] = RegMeta(fpath, kind, None, None, enc)
                elif kind == FileKind.EXCEL:
                    try:
                        try:
                            self.con.execute("LOAD excel;")
                        except Exception:
                            pass
                        sh = sheet or 0
                        self.con.execute(
                            f"""
                            CREATE OR REPLACE VIEW {_esc_ident(schema)}.{_esc_ident(internal)} AS
                            SELECT * FROM read_excel('{fpath.as_posix()}', sheet='{sh}');
                            """
                        )
                        self.registry[(schema, internal)] = RegMeta(fpath, kind, sheet, None, None)
                    except Exception:
                        try:
                            df = pd.read_excel(fpath, sheet_name=sheet or 0, engine="openpyxl")
                        except Exception:
                            df = pd.read_excel(fpath, sheet_name=sheet or 0)
                        tmp = f"tmp_{schema}_{internal}"
                        self.con.register(tmp, df)
                        self.con.execute(
                            f"""
                            CREATE OR REPLACE TABLE {_esc_ident(schema)}.{_esc_ident(internal)} AS
                            SELECT * FROM {_esc_ident(tmp)};
                            """
                        )
                        self.con.unregister(tmp)
                        self.registry[(schema, internal)] = RegMeta(fpath, kind, sheet, None, None)
            except Exception as e:
                print("[WARN] skip:", fpath, e)

    def describe(self, schema, internal) -> DataFrame:
        df = self.con.execute(
            f"DESCRIBE {_esc_ident(schema)}.{_esc_ident(internal)};"
        ).fetchdf()
        if "column_name" in df.columns:
            return df.rename(columns={"column_name": "colname"})
        if "name" in df.columns:
            return df.rename(columns={"name": "colname"})
        for c in df.columns:
            if c.lower() in ("column_name", "name"):
                return df.rename(columns={c: "colname"})
        return df

    def preview(self, schema, table_internal, limit=100) -> DataFrame:
        return self.con.execute(
            f"SELECT * FROM {_esc_ident(schema)}.{_esc_ident(table_internal)} LIMIT {limit};"
        ).fetchdf()

    def _backup(self, path: Path):
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            bak = path.with_suffix(path.suffix + f".{ts}.bak")
            shutil.copy2(path, bak)
        except Exception:
            pass

    def maybe_ctas(self, stmt: str) -> str | None:
        m = self._re_ctas.match(stmt)
        if not m:
            return None
        schema, table, select_sql = m.group(1), m.group(2), m.group(3)
        if schema not in self.schemas:
            raise RuntimeError(f"Unknown schema: {schema}")
        out = (self.schemas[schema] / f"{table}.csv")
        if out.exists():
            self._backup(out)
        self.con.execute(
            f"COPY ({select_sql}) TO '{out.as_posix()}' (HEADER, DELIMITER ',');"
        )
        self.con.execute(f"DROP SCHEMA IF EXISTS {_esc_ident(schema)} CASCADE;")
        self.attach_folder(schema, self.schemas[schema])
        return str(out)

    def maybe_write_back(self, stmt: str) -> bool:
        if not self._re_dml.search(stmt or ""): return False
        m = self._re_target.search(stmt)
        if not m: return False
        schema, table = m.group(1), m.group(2)
        meta = self.registry.get((schema,table))
        if not meta: return False

        self.con.execute("DROP TABLE IF EXISTS __edit_tmp__;")
        self.con.execute(
            f"CREATE TEMP TABLE __edit_tmp__ AS SELECT * FROM {_esc_ident(schema)}.{_esc_ident(table)};"
        )

        def _rep(mm):
            frag = mm.group(0)
            return re.sub(rf"\b{schema}\.{table}\b", "__edit_tmp__", frag, flags=re.I)

        stmt2 = re.sub(r"(?i)\b(INTO|UPDATE|FROM)\s+[A-Za-z_]\w*\.[A-Za-z_]\w*", _rep, stmt)
        self.con.execute(stmt2)
        df = self.con.execute("SELECT * FROM __edit_tmp__;").fetchdf()
        if "filename" in df.columns:
            df.drop(columns=["filename"], inplace=True)

        self._backup(meta.path)
        try:
            if meta.kind in (FileKind.CSV, FileKind.TXT):
                delim = meta.delimiter or sniff_delimiter(meta.path)
                enc   = meta.encoding or "utf-8"
                tmp = meta.path.with_suffix(meta.path.suffix + ".tmp")
                df.to_csv(tmp, index=False, encoding=enc, sep=delim)
                os.replace(tmp, meta.path)
            else:
                if openpyxl is None: raise RuntimeError("openpyxl needed for Excel write-back.")
                if meta.path.suffix.lower()==".xls": raise RuntimeError("Write .xls not supported, save as .xlsx.")
                sheet = meta.sheet or "Sheet1"
                with pd.ExcelWriter(meta.path, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
                    df.to_excel(w, index=False, sheet_name=sheet)
        except PermissionError:
            raise RuntimeError("Write failed. Close the file in Excel and try again.")

        self.con.execute(f"DROP SCHEMA IF EXISTS {_esc_ident(schema)} CASCADE;")
        self.attach_folder(schema, meta.path.parent)
        return True

    def run_query_limited(self, sql: str, cap: int|None):
        if cap and re.match(r"^\s*select\b", sql, re.I) and re.search(r"\blimit\b", sql, re.I) is None:
            sql = f"SELECT * FROM ({sql}) __t LIMIT {cap}"
        return self.con.execute(sql).fetchdf()

SQL_FONT = ("Consolas", 11) if sys.platform.startswith("win") else ("Menlo", 12)
class EditorTab(ttk.Frame):
    """Independent editor tab: Text + line numbers"""
    _seq = 1
    def __init__(self, master, title=None):
        super().__init__(master)
        self.path: Path|None = None
        self.dirty = False

        wrap = ttk.Frame(self); wrap.pack(fill=tk.BOTH, expand=True)
        
        self.linenum = tk.Canvas(wrap, width=44, background="#f5f5f5", highlightthickness=0)
        
        self.text = tk.Text(wrap, height=10, wrap="none", undo=True, font=SQL_FONT)
        q_vsb = ttk.Scrollbar(wrap, orient="vertical", command=self.text.yview)
        q_hsb = ttk.Scrollbar(wrap, orient="horizontal", command=self.text.xview)
        
        wrap.rowconfigure(0, weight=1)
        wrap.columnconfigure(1, weight=1)
        
        self.linenum.grid(row=0, column=0, sticky="ns")
        self.text.grid(row=0, column=1, sticky="nsew")
        q_vsb.grid(row=0, column=2, sticky="ns")
        q_hsb.grid(row=1, column=1, sticky="ew")
        
        self.text.configure(
            yscrollcommand=lambda *a: (q_vsb.set(*a), self.update_linenum()),
            xscrollcommand=q_hsb.set
        )

        self.title = title or f"Query {EditorTab._seq}"
        EditorTab._seq += 1

    def set_dirty(self, v: bool):
        self.dirty = v

    def update_linenum(self):
        self.linenum.delete("all")
        lines = self.text.get("1.0", tk.END).count("\n") + 1
        for i in range(1, lines+1):
            y = self.text.dlineinfo(f"{i}.0")
            if y: self.linenum.create_text(2, y[1], anchor="nw", text=str(i), fill="#999")


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry(load_settings().get("geometry", "1400x900"))
        self.minsize(1180, 760)
        self.servers: list[Path] = []
        self._srv_seq: int = 1
        self._server_alias: dict[str, str] = {}
        self._schemas_by_server: dict[str, list[str]] = {}
        self._server_node_by_path: dict[str, str] = {}

        self.catalog = DuckCatalog()
        self.current_sql_path: Path | None = None
        self._text_dirty = False

        self._last_df_original: DataFrame | None = None
        self._last_df_current:  DataFrame | None = None
        self._result_sort_state = {}
        self._last_clicked_column_index = 0

        self._dark = bool(load_settings().get("dark", False))
        self._catalog_cols_cache = {}

        self._build_menubar()
        self._build_toolbar()
        self._build_main_area()
        self._bind_shortcuts()
        self._refresh_recent_menu()
        if self._dark: self._apply_dark()

    def _find_server_node_up(self, node):
        cur = node
        while cur:
            vals = self.tree.item(cur, "values")
            if vals and vals[0] in {"server", "database"}:
                path_str = vals[1] if len(vals) > 1 else self.tree.item(cur, "text")
                return cur, path_str
            cur = self.tree.parent(cur)
        return None, None

    def _disconnect_by_path(self, path: Path):
        spath = str(path)
        alias = self._server_alias.pop(spath, None)
        schemas = self._schemas_by_server.pop(alias, []) if alias else []
        for sch in schemas:
            try:
                self.catalog.drop_schema(sch)
            except Exception:
                pass
        self.servers = [p for p in self.servers if str(p) != spath]
        node_id = self._server_node_by_path.pop(spath, None)
        if node_id and self.tree.exists(node_id):
            self.tree.delete(node_id)
        self.status_var.set(f"Disconnected: {spath}")
        if not self.servers:
            for b in (self.btn_refresh, self.btn_csv, self.btn_xlsx, self.btn_json,
                      self.btn_copy, self.btn_prof, self.btn_run, self.btn_run_current, self.btn_undo):
                b.config(state=tk.DISABLED)

    def disconnect_selected_server(self):
        path = self._get_selected_server_path()
        if path:
            self._disconnect_by_path(path)

    def disconnect_all_servers(self):
        if not self.servers:
            self.status_var.set("No folders connected.")
            return
        self.servers.clear()
        self._server_alias.clear()
        self._schemas_by_server.clear()
        self.catalog.reset()
        self.tree.delete(*self.tree.get_children())
        self._catalog_cols_cache.clear()
        for b in (self.btn_refresh, self.btn_csv, self.btn_xlsx, self.btn_json,
                  self.btn_copy, self.btn_prof, self.btn_run, self.btn_run_current, self.btn_undo):
            b.config(state=tk.DISABLED)
        self.status_var.set("All folders disconnected.")
    
    def _open_path(self, path: str):
        try:
            os.startfile(path)
        except Exception as e:
            messagebox.showerror("Open Folder", str(e))
    
    def _copy_to_clip(self, text: str, toast: str = "Copied"):
        self.clipboard_clear(); self.clipboard_append(text); self.update()
        self.status_var.set(toast)

    def _add_server(self, folder: Path):
        alias = f"s{self._srv_seq}"; self._srv_seq += 1
        self.servers.append(folder)
        self._server_alias[str(folder)] = alias
        self._schemas_by_server[alias] = []
    
        server_node = self.tree.insert("", "end", text=str(folder), open=True,
                                       values=("server", str(folder)))
        self._server_node_by_path[str(folder)] = server_node
        self._attach_server_into_catalog(folder, alias, server_node)
        for b in (self.btn_refresh, self.btn_csv, self.btn_xlsx, self.btn_json,
                  self.btn_copy, self.btn_prof, self.btn_run, self.btn_run_current, self.btn_undo):
            b.config(state=tk.NORMAL)
        self.status_var.set(f"Connected: {folder}  (alias {alias})")

    def _attach_server_into_catalog(self, root: Path, alias: str, server_node):
        """
        Scans the root folder and its subfolders:
          - Root files ⇒ schema '{alias}_root' (always created even if empty)
          - Each subfolder ⇒ schema '{alias}_{safe(subfolder)}'
        Adds tables under the appropriate tree nodes.
        """
        entries = list(root.iterdir())
        dbs = []
        dbs.append((f"{alias}_root", root, "root"))
        for p in sorted(entries):
            if p.is_dir():
                dbs.append((f"{alias}_{_to_safe_schema(p.name)}", p, p.name))
        for schema_name, db_path, display_name in dbs:
            db_node = self.tree.insert(
                server_node, "end",
                text=str(display_name), open=False,
                values=("database", schema_name, str(db_path))
            )
            self.catalog.attach_folder(schema_name, db_path)
            self._schemas_by_server[alias].append(schema_name)
            added = set()
            for display_name_tbl, fpath, kind, sheet in iter_tables_in_path(db_path):
                if display_name_tbl in added: 
                    continue
                added.add(display_name_tbl)
                internal = self.catalog.names.to_internal(schema_name, display_name_tbl)
                ident = f"{schema_name}.{internal}"
                title = display_name_tbl if not (kind == FileKind.EXCEL and sheet) else f"{display_name_tbl} (excel)"
                self.tree.insert(db_node, "end", text=title, values=(ident,))

    def _get_selected_server_path(self) -> Path | None:
        """
        Returns the server (folder) path regardless of selected node type:
        - If the node itself is "server" ⇒ use its value.
        - If "database" (schema) ⇒ take folder path from values[2].
        - If a table ⇒ climb up until reaching the server node.
        """
        node = self.tree.focus()
        while node:
            vals = self.tree.item(node, "values") or ()
            kind = vals[0] if len(vals) >= 1 else ""
            if kind == "server":
                path_str = vals[1] if len(vals) > 1 else self.tree.item(node, "text")
                try:
                    return Path(path_str)
                except Exception:
                    return None
            if kind == "database":
                if len(vals) >= 3:
                    try:
                        return Path(vals[2])
                    except Exception:
                        return None
            node = self.tree.parent(node)
        return None

    def _get_active_editor_title(self) -> str:
        cur = self.ed_nb.select()
        tab = self._editor_tabs.get(cur)
        if not tab:
            return "Query"
        t = tab.title.lstrip("* ").strip()
        return t or "Query"

    def _build_menubar(self):
        self.menubar = tk.Menu(self); self.config(menu=self.menubar)
    
        m_file = tk.Menu(self.menubar, tearoff=False)
        m_file.add_command(label="New Query", accelerator="Ctrl+N", command=self.file_new)
        m_file.add_command(label="Open…", accelerator="Ctrl+O", command=self.file_open)
        m_file.add_command(label="Save", accelerator="Ctrl+S", command=self.file_save)
        m_file.add_command(label="Save As…", command=self.file_save_as)
        m_file.add_separator(); m_file.add_command(label="Exit", command=self._on_close)
        self.menubar.add_cascade(label="File", menu=m_file)
    
        m_db = tk.Menu(self.menubar, tearoff=False)
        m_db.add_command(label="Add Database (Folder)", command=self.choose_server)
        m_db.add_separator()
        m_db.add_command(label="Disconnect Selected", command=self.disconnect_selected_server)
        m_db.add_command(label="Disconnect All", command=self.disconnect_all_servers)
        self.m_recent = tk.Menu(m_db, tearoff=False); m_db.add_cascade(label="Recent Databases", menu=self.m_recent)
        m_db.add_separator()
        m_db.add_command(label="New Database (Create Folder)", command=self.create_database_folder)
        self.menubar.add_cascade(label="Database", menu=m_db)
    
        m_tools = tk.Menu(self.menubar, tearoff=False)
        m_tools.add_command(label="Column Profiler", command=self.profile_dialog)
        m_tools.add_command(label="Refresh Catalog", command=self.refresh_catalog)
        m_tools.add_command(label="Undo Last Write", command=self.undo_last_write)
        m_tools.add_separator(); m_tools.add_command(label="DuckDB Docs", command=lambda: webbrowser.open("https://duckdb.org/docs/"))
        self.menubar.add_cascade(label="Tools", menu=m_tools)
    
        m_view = tk.Menu(self.menubar, tearoff=False)
        m_view.add_command(label="Toggle Dark Mode", command=self._toggle_dark)
        self.menubar.add_cascade(label="View", menu=m_view)
    
        m_help = tk.Menu(self.menubar, tearoff=False)
        m_help.add_command(label="About", command=lambda: messagebox.showinfo("About", APP_TITLE))
        self.menubar.add_cascade(label="Help", menu=m_help)

    def _refresh_recent_menu(self):
        self.m_recent.delete(0, tk.END)
        recs = load_recents()
        if not recs:
            self.m_recent.add_command(label="(empty)", state=tk.DISABLED); return
        for p in recs:
            self.m_recent.add_command(label=p, command=lambda p=p: self._connect_to_recent(p))

    def _add_tip(self, widget, *texts):
        """
        Adds a tooltip to any widget.
        Supports passing multiple texts and merges them into lines.
        """
        text = "\n".join([t for t in texts if t]).strip()
        if not text or widget is None:
            return
        try:
            _ToolTip(widget, text, delay=400, wrap=420)
        except Exception:
            pass

    def _build_toolbar(self):
        tb = ttk.Frame(self); tb.pack(side=tk.TOP, fill=tk.X)
    
        self.btn_connect = ttk.Button(tb, text="Add Database (Folder)", command=self.choose_server)
        self.btn_connect.pack(side=tk.LEFT, padx=6, pady=6)
        self.btn_refresh = ttk.Button(tb, text="Refresh Catalog", command=self.refresh_catalog, state=tk.DISABLED)
        self.btn_refresh.pack(side=tk.LEFT, padx=6)
        self.status_var = tk.StringVar(value="Add a database (folder) to begin…")
    
        self.btn_export_csv  = ttk.Button(tb, text="Export CSV",  command=self.export_result_csv,  state=tk.DISABLED);  self.btn_export_csv.pack(side=tk.LEFT, padx=6)
        self.btn_export_xlsx = ttk.Button(tb, text="Export Excel",command=self.export_result_xlsx, state=tk.DISABLED);  self.btn_export_xlsx.pack(side=tk.LEFT, padx=6)
        self.btn_export_json = ttk.Button(tb, text="Export JSON", command=self.export_result_json, state=tk.DISABLED);  self.btn_export_json.pack(side=tk.LEFT, padx=6)
        self.btn_copy_clip   = ttk.Button(tb, text="Copy TSV",    command=self.copy_result_to_clipboard, state=tk.DISABLED); self.btn_copy_clip.pack(side=tk.LEFT, padx=6)
    
        self.btn_profile = ttk.Button(tb, text="Column Profiler", command=self.profile_dialog, state=tk.DISABLED)
        self.btn_profile.pack(side=tk.LEFT, padx=6)
    
        self.btn_undo = ttk.Button(tb, text="Undo Last Write", command=self.undo_last_write, state=tk.DISABLED)
        self.btn_undo.pack(side=tk.LEFT, padx=6)
    
        ttk.Label(tb, textvariable=self.status_var, anchor="e").pack(side=tk.RIGHT, padx=8)
    
        self._add_tip(self.btn_connect, "Select a folder and treat it as a Server to load files as tables", "Connect to a folder")
        self._add_tip(self.btn_refresh, "Reload the catalog (re-scan files)", "Refresh catalog")
        self._add_tip(self.btn_export_csv, "Export the latest result to CSV (UTF-8-SIG)", "Export CSV")
        self._add_tip(self.btn_export_xlsx, "Export the latest result to Excel (requires openpyxl)", "Export Excel")
        self._add_tip(self.btn_export_json, "Export the latest result to JSON (records)", "Export JSON")
        self._add_tip(self.btn_copy_clip, "Copy the latest result to clipboard as TSV with headers", "Copy TSV")
        self._add_tip(self.btn_profile, "Analyze columns for the current result (count/unique/min/max…)", "Column profiler")
        self._add_tip(self.btn_undo, "Restore the latest .bak before write operations", "Undo last write")
        self._add_tip(self.btn_connect, "Add a database (folder) to treat files as tables", "Add Database")
        self._add_tip(self.btn_refresh, "Update the catalog and reload files", "Refresh catalog")
    
        self.btn_csv  = self.btn_export_csv
        self.btn_xlsx = self.btn_export_xlsx
        self.btn_json = self.btn_export_json
        self.btn_copy = self.btn_copy_clip
        self.btn_prof = self.btn_profile

    def _build_main_area(self):
        main = ttk.Panedwindow(self, orient=tk.HORIZONTAL); main.pack(fill=tk.BOTH, expand=True)
    
        left = ttk.Frame(main); main.add(left, weight=1)
        self.tree = ttk.Treeview(left, columns=("ident",), show="tree")
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.tree.bind("<Double-1>", self.on_tree_double)
        self.tree.bind("<Button-3>", self.on_tree_context)
        tv_v = ttk.Scrollbar(left, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=tv_v.set); tv_v.pack(side=tk.RIGHT, fill=tk.Y)
    
        right = ttk.Panedwindow(main, orient=tk.VERTICAL); main.add(right, weight=3)
    
        ed_host = ttk.Frame(right); right.add(ed_host, weight=1)
    
        edbar = ttk.Frame(ed_host); edbar.pack(side=tk.TOP, fill=tk.X)
        self.btn_run = ttk.Button(edbar, text="Run (F5)", command=self.run_query, state=tk.DISABLED); self.btn_run.pack(side=tk.LEFT, padx=6, pady=4)
        self.btn_run_current = ttk.Button(edbar, text="Run Current (Ctrl+Enter)", command=self.run_current_stmt, state=tk.DISABLED); self.btn_run_current.pack(side=tk.LEFT, padx=6)
        self.btn_clear = ttk.Button(edbar, text="Clear", command=self.clear_query); self.btn_clear.pack(side=tk.LEFT)
        self.btn_examples = ttk.Button(edbar, text="Examples", command=self.insert_examples); self.btn_examples.pack(side=tk.LEFT, padx=6)
        self.btn_comment = ttk.Button(edbar, text="Comment/Uncomment (Ctrl+/)", command=self.toggle_comment); self.btn_comment.pack(side=tk.LEFT, padx=6)
        self.btn_find = ttk.Button(edbar, text="Find (Ctrl+F)", command=self.find_in_editor); self.btn_find.pack(side=tk.LEFT, padx=6)
    
        self._add_tip(self.btn_run, "Run the entire script (supports ; and GO). Respects Safe Mode", "Run script")
        self._add_tip(self.btn_run_current, "Execute only the current statement at the cursor", "Run current statement")
        self._add_tip(self.btn_clear, "Clear the editor content", "Clear editor")
        self._add_tip(self.btn_examples, "Insert ready-made examples", "Insert examples")
        self._add_tip(self.btn_comment, "Comment/Uncomment", "Toggle comment")
        self._add_tip(self.btn_find, "Find and replace", "Find/Replace")
    
        self.ed_nb = ttk.Notebook(ed_host)
        self.ed_nb.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.ed_nb.bind("<<NotebookTabChanged>>", lambda e: self._on_editor_tab_changed())
        self.ed_nb.bind("<Button-3>", self._editor_tab_menu)

        self._editor_tabs: dict[str, EditorTab] = {}
        self._new_editor_tab(initial_text="-- Write SQL here.\n")
    
        nbframe = ttk.Frame(right); right.add(nbframe, weight=2)
    
        self.nb = ttk.Notebook(nbframe); self.nb.pack(fill=tk.BOTH, expand=True)
        self.tab_results  = ttk.Frame(self.nb); self.nb.add(self.tab_results, text="Results")
        self.tab_messages = ttk.Frame(self.nb); self.nb.add(self.tab_messages, text="Messages")
    
        self.res_nb = ttk.Notebook(self.tab_results)
        self.res_nb.pack(fill=tk.BOTH, expand=True)
        self.res_nb.bind("<<NotebookTabChanged>>", lambda e: self._on_result_tab_changed())
        self.res_nb.bind("<Button-3>", self._result_tab_menu)
    
        self._res_tabs: dict[str, dict] = {}
    
        msg = ttk.Frame(self.tab_messages); msg.pack(fill=tk.BOTH, expand=True)
        self.msgbox = tk.Text(msg, wrap="word", height=6, font=("Consolas", 10))
        self.msgbox.pack(fill=tk.BOTH, expand=True)

    def _new_editor_tab(self, title=None, initial_text: str="-- Write SQL here.\n"):
        tab = EditorTab(self.ed_nb, title=title)
        tab.text.insert("1.0", initial_text)
        tab.text.edit_modified(False)
    
        tab.text.bind("<<Modified>>",        lambda e, t=tab: self._on_text_modified_tab(t))
        tab.text.bind("<KeyRelease>",        lambda e, t=tab: (self._apply_sql_highlighting(t), t.update_linenum()))
        tab.text.bind("<ButtonRelease>",     lambda e, t=tab: (self._apply_sql_highlighting(t), t.update_linenum()))
    
        self._init_sql_highlighting(tab)
    
        self.ed_nb.add(tab, text=tab.title)
        self._editor_tabs[str(tab)] = tab
        self.ed_nb.select(tab)
        self._set_active_editor(tab)
    
        if hasattr(self, "_ac"):
            tab.text.bind("<KeyRelease-period>", lambda e: _ac_trigger(self), add=True)
            tab.text.bind("<Up>",     lambda e: (self._ac.list.event_generate("<Up>"),     "break")[1] if self._ac.is_visible() else None, add=True)
            tab.text.bind("<Down>",   lambda e: (self._ac.list.event_generate("<Down>"),   "break")[1] if self._ac.is_visible() else None, add=True)
            tab.text.bind("<Return>", lambda e: (self._ac.list.event_generate("<Return>"), "break")[1] if self._ac.is_visible() else None, add=True)
            tab.text.bind("<Tab>",    lambda e: (self._ac.list.event_generate("<Tab>"),    "break")[1] if self._ac.is_visible() else None, add=True)
            tab.text.bind("<Button-1>", lambda e: self._ac.hide(), add=True)
            tab.text.bind("<Key>",      lambda e: self._ac.hide(), add=True)
    
        self.btn_run.config(state=tk.NORMAL)
        self.btn_run_current.config(state=tk.NORMAL)
        return tab
    
    def _close_editor_tab(self, tab_id=None):
        cur = tab_id or self.ed_nb.select()
        if not cur:
            return
        tab = self._editor_tabs.get(cur)
        if tab and tab.dirty:
            r = messagebox.askyesnocancel("Unsaved", f"Save changes in {tab.title}?")
            if r is None:
                return
            if r:
                self._set_active_editor(tab)
                self.file_save()
        self.ed_nb.forget(cur)
        self._editor_tabs.pop(cur, None)
    
        if not self.ed_nb.tabs():
            self._new_editor_tab()
    
    def _on_editor_tab_changed(self):
        cur = self.ed_nb.select()
        tab = self._editor_tabs.get(cur)
        if tab:
            self._set_active_editor(tab)
    
    def _set_active_editor(self, tab: 'EditorTab'):
        self.editor = tab.text
        self.linenum = tab.linenum
        self._apply_sql_highlighting(tab)
        tab.update_linenum()
    
    def _on_text_modified_tab(self, tab: 'EditorTab'):
        if tab.text.edit_modified():
            tab.set_dirty(True)
            i = self.ed_nb.index(tab)
            cap = self.ed_nb.tab(i, "text")
            if not cap.startswith("* "):
                self.ed_nb.tab(i, text="* " + tab.title)
            tab.text.edit_modified(False)
            if getattr(self, "editor", None) is tab.text:
                self._set_dirty(True)
            tab.update_linenum()

    def _bind_shortcuts(self):
        self.bind("<F5>", lambda e: self.run_query())
        self.bind("<Control-Return>", lambda e: self.run_current_stmt())
        self.bind("<Control-n>", lambda e: self.file_new()); self.bind("<Control-N>", lambda e: self.file_new())
        self.bind("<Control-o>", lambda e: self.file_open()); self.bind("<Control-O>", lambda e: self.file_open())
        self.bind("<Control-s>", lambda e: self.file_save()); self.bind("<Control-S>", lambda e: self.file_save())
        self.bind("<Control-slash>", lambda e: self.toggle_comment())
        self.bind("<Control-Tab>", lambda e: self.nb.select(self.tab_messages if self.nb.select()==str(self.tab_results) else self.tab_results))
        self.bind("<F3>",        lambda e: self._find_next())
        self.bind("<Shift-F3>",  lambda e: self._find_prev())
        self.bind("<Control-h>", lambda e: self.find_in_editor())
        self.bind("<F3>",        lambda e: self._find_next())
        self.bind("<Shift-F3>",  lambda e: self._find_prev())
        self.bind("<Control-t>", lambda e: self._new_editor_tab())
        self.bind("<Control-w>", lambda e: self._close_editor_tab())

    def _on_close(self):
        st = load_settings()
        st["geometry"] = self.geometry()
        st["dark"] = self._dark
        save_settings(st)
        self.destroy()

    def file_new(self):
        self._new_editor_tab(initial_text="-- Write SQL here.\n")
    
    def file_open(self):
        fp = filedialog.askopenfilename(title="Open SQL", filetypes=[("SQL",".sql"),("All","*.*")])
        if not fp: return
        txt = open(fp,"r",encoding="utf-8").read()
        self._new_editor_tab(title=Path(fp).name, initial_text=txt)
        tab = self._editor_tabs[self.ed_nb.select()]
        tab.path = Path(fp)
        tab.set_dirty(False)
        self._init_sql_highlighting(tab); self._apply_sql_highlighting(tab); tab.update_linenum()
    
    def file_save(self):
        cur = self.ed_nb.select(); tab = self._editor_tabs.get(cur)
        if not tab: return
        if not tab.path: return self.file_save_as()
        open(tab.path,"w",encoding="utf-8").write(tab.text.get("1.0", tk.END))
        tab.set_dirty(False)
        i = self.ed_nb.index(tab); self.ed_nb.tab(i, text=tab.title)
        self._set_dirty(False)
        self.status_var.set(f"Saved: {tab.path}")
    
    def file_save_as(self):
        cur = self.ed_nb.select(); tab = self._editor_tabs.get(cur)
        if not tab: return
        fp = filedialog.asksaveasfilename(title="Save SQL As", defaultextension=".sql", filetypes=[("SQL",".sql")])
        if not fp: return
        tab.path = Path(fp); tab.title = Path(fp).name
        self.file_save()

    def _maybe_prompt_save(self) -> bool:
        cur = self.ed_nb.select(); tab = self._editor_tabs.get(cur)
        if not tab or not tab.dirty: return True
        r = messagebox.askyesnocancel("Unsaved Changes", f"Save {tab.title}?")
        if r is None: return False
        if r: self.file_save()
        return True

    def _connect_to_recent(self, p: str):
        path = Path(p)
        if not path.exists():
            messagebox.showwarning("Missing", f"Path not found:\n{p}")
            return
        if any(str(path).lower() == str(s).lower() for s in self.servers):
            self.status_var.set("This folder is already connected.")
            return
        self._add_server(path)

    def choose_server(self):
        p = filedialog.askdirectory(title="Choose Folder (Server)")
        if not p:
            return
        path = Path(p)
        if any(str(path).lower() == str(s).lower() for s in self.servers):
            self.status_var.set("This folder is already connected.")
            return
        add_recent(path); self._refresh_recent_menu()
        self._add_server(path)

    def create_database_folder(self):
        name = self._ask_string("New Database", "Enter database (folder) name:")
        if not name:
            return
        parent = filedialog.askdirectory(title="Choose parent folder for the new database")
        if not parent:
            return
        target = Path(parent) / _to_safe_schema(name)
        try:
            target.mkdir(parents=True, exist_ok=False)
            messagebox.showinfo("New Database", f"Created: {target}")
        except FileExistsError:
            if messagebox.askyesno("Exists", f"Folder already exists:\n{target}\n\nConnect to it?"):
                pass
            else:
                return
        except Exception as e:
            messagebox.showerror("New Database", str(e))
            return
        if not any(str(target).lower() == str(s).lower() for s in self.servers):
            add_recent(target); self._refresh_recent_menu()
            self._add_server(target)
        else:
            self.status_var.set("Database already connected.")

    def refresh_catalog(self):
        if not self.servers:
            return
        for b in (self.btn_refresh, self.btn_csv, self.btn_xlsx, self.btn_json,
                  self.btn_copy, self.btn_prof, self.btn_run, self.btn_run_current, self.btn_undo):
            b.config(state=tk.DISABLED)
        self._result_close_all()
        self.msgbox.delete("1.0", tk.END)
        self.catalog.reset()
        self._catalog_cols_cache.clear()
        self.tree.delete(*self.tree.get_children())
        self._server_node_by_path.clear()
        for srv in self.servers:
            alias = self._server_alias.get(str(srv))
            if not alias:
                alias = f"s{self._srv_seq}"; self._srv_seq += 1
                self._server_alias[str(srv)] = alias
                self._schemas_by_server[alias] = []
            server_node = self.tree.insert("", "end", text=str(srv), open=True,
                                           values=("server", str(srv)))
            self._server_node_by_path[str(srv)] = server_node
            self._schemas_by_server[alias] = []
            self._attach_server_into_catalog(srv, alias, server_node)
        for b in (self.btn_refresh, self.btn_csv, self.btn_xlsx, self.btn_json,
                  self.btn_copy, self.btn_prof, self.btn_run, self.btn_run_current, self.btn_undo):
            b.config(state=tk.NORMAL)
        self.status_var.set("Ready.")

    def on_tree_double(self, event):
        node = self.tree.focus()
        vals = self.tree.item(node, "values") if node else ()
        if not vals: return
        ident = vals[0]
        if ident in {"database", "subfolder"}: return
        schema, table = ident.split(".",1)
        try:
            df = self.catalog.preview(schema, table, limit=100)
            self.results_show_dataframe(df)
            self.nb.select(self.tab_results)
            self.status_var.set(f"Preview: {schema}.{table} (100 rows)")
        except Exception as e:
            messagebox.showerror("Preview Error", str(e))

    def on_tree_context(self, event):
        node = self.tree.identify_row(event.y)
        if not node:
            return
        self.tree.selection_set(node)
        vals = self.tree.item(node, "values") or ()
        kind = vals[0] if len(vals) >= 1 else ""
        menu = tk.Menu(self, tearoff=False)
        if kind in {"server", "database"}:
            schema_name = vals[1] if (kind == "database" and len(vals) > 1) else ""
            folder_path = (
                vals[2] if (kind == "database" and len(vals) > 2)
                else (vals[1] if (kind == "server" and len(vals) > 1) else self.tree.item(node, "text"))
            )
            if schema_name:
                menu.add_command(
                    label="Copy Schema Name",
                    command=lambda s=schema_name: self._copy_to_clip(s, "Schema copied")
                )
            if folder_path:
                menu.add_command(
                    label="Open Folder",
                    command=lambda p=folder_path: self._open_path(p)
                )
                menu.add_command(
                    label="Disconnect",
                    command=lambda p=Path(folder_path): self._disconnect_by_path(p)
                )
            if schema_name:
                menu.add_separator()
                def _insert_ctas_template():
                    tmpl = (
                        "\n-- CTAS into this schema (creates my_table.csv under this folder)\n"
                        f'CREATE TABLE {schema_name}."my_table" AS\n'
                        "SELECT 1 AS id, 'ok' AS note;\n"
                    )
                    self.editor.insert(tk.END, tmpl)
                    self._apply_sql_highlighting()
                menu.add_command(label="Insert CTAS template here", command=_insert_ctas_template)
            menu.post(event.x_root, event.y_root)
            return
        ident = vals[0] if vals else ""
        if not ident or "." not in ident:
            return
        schema, table = ident.split(".", 1)
        menu.add_command(label="Preview Top 100", command=lambda s=schema, t=table: self._ctx_preview(s, t))
        menu.add_command(label="Describe",        command=lambda s=schema, t=table: self._ctx_describe(s, t))
        menu.add_command(label="Profile",         command=lambda s=schema, t=table: self.profile_dialog(f"{s}.{t}"))
        menu.post(event.x_root, event.y_root)

    def _ctx_preview(self, schema, table):
        try:
            df = self.catalog.preview(schema, table, limit=100)
            self.results_show_dataframe(df)
            self.status_var.set(f"Preview: {schema}.{table} (100 rows)")
        except Exception as e:
            messagebox.showerror("Preview Error", str(e))

    def _ctx_describe(self, schema, table):
        try:
            df = self.catalog.describe(schema, table)
            self.results_show_dataframe(df)
            self.status_var.set(f"DESCRIBE {schema}.{table}")
        except Exception as e:
            messagebox.showerror("Describe Error", str(e))

    def _normalize_for_grid(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for c in out.columns:
            s = out[c]
            try:
                if pd.api.types.is_datetime64_any_dtype(s) or pd.api.types.is_datetime64_dtype(s):
                    out[c] = pd.to_datetime(s, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
                    out.loc[pd.isna(s), c] = ""
                elif pd.api.types.is_timedelta64_dtype(s):
                    out[c] = s.astype("string").fillna("")
                else:
                    out[c] = s.astype(object)
                    out.loc[pd.isna(s), c] = ""
            except Exception:
                out[c] = s.astype(object).where(~pd.isna(s), "")
        return out
    
    def _apply_result_dataframe_to_grid(self, tree: ttk.Treeview, df: pd.DataFrame, state: dict):
        disp = self._normalize_for_grid(df)
        disp_display = disp.copy()
        disp_display.insert(0, "#", [i for i in range(1, len(disp_display) + 1)])
    
        tree.delete(*tree.get_children())
        cols_display = ["#"] + [str(c) for c in disp.columns]
        tree["columns"] = cols_display
    
        def _mk_sort(colname):
            return lambda c=colname: self._result_sort_by(c)
    
        for c in cols_display:
            if c == "#":
                tree.heading(c, text=c, anchor="w")
                tree.column(c, width=60, anchor="w", stretch=False)
                continue
            tree.heading(c, text=c, anchor="w", command=_mk_sort(c))
            try:
                sample_len = int(disp[c].astype(str).map(len).quantile(0.90)) + 2
            except Exception:
                sample_len = max(12, len(c) + 2)
            width_px = max(120, min(600, sample_len * 8))
            tree.column(c, width=width_px, anchor="w", stretch=False)
    
        to_str = lambda v: "" if (v is None or (isinstance(v, float) and pd.isna(v))) else str(v)
        for i, row in enumerate(disp_display.itertuples(index=False, name=None)):
            tree.insert("", "end", iid=str(i), values=[to_str(v) for v in row])
    
        state["df_cur"] = disp.copy()
        for b in (self.btn_csv, self.btn_xlsx, self.btn_json, self.btn_copy, self.btn_prof):
            b.config(state=tk.NORMAL)
        self.status_var.set(f"Rows: {len(disp)}")

    def results_show_dataframe(self, df: pd.DataFrame, title: str|None=None):
        title = title or self._get_active_editor_title() or "Result"
        frm = self._create_result_tab(title)
        tree, state = self._get_active_result_tree()
        state["df_orig"] = df.copy()
        state["sort"].clear()
        self._apply_result_dataframe_to_grid(tree, df, state)
        self.nb.select(self.tab_results)

    def _result_sort_by(self, col: str):
        tree, state = self._get_active_result_tree()
        if not tree or not state or state["df_orig"] is None: return
        prev = state["sort"].get(col)
        new_state = "asc" if prev is None else ("desc" if prev == "asc" else None)
        state["sort"].clear(); state["sort"][col] = new_state
    
        if new_state is None:
            df = state["df_orig"].copy()
        else:
            ascending = (new_state == "asc")
            try:
                df = state["df_orig"].sort_values(by=col, ascending=ascending, kind="mergesort")
            except Exception:
                df2 = state["df_orig"].copy()
                df2[col] = df2[col].astype(str)
                df = df2.sort_values(by=col, ascending=ascending, kind="mergesort")
    
        self._apply_result_dataframe_to_grid(tree, df, state)
        arrow = " ▲" if new_state=="asc" else (" ▼" if new_state=="desc" else "")
        for c in tree["columns"]:
            tree.heading(c, text=c + (arrow if c==col and new_state else ""))

    def _grid_context(self, event, tree=None):
        tree = tree or (self._get_active_result_tree()[0])
        if not tree: return
        iid = tree.identify_row(event.y)
        col = tree.identify_column(event.x)
        if iid: tree.selection_set(iid)
        menu = tk.Menu(self, tearoff=False)
        menu.add_command(label="Copy Cell", command=lambda: self._copy_cell(tree=tree))
        menu.add_command(label="Copy Row", command=lambda: self._copy_row(iid, tree=tree))
        menu.add_command(label="Copy Column", command=lambda: self._copy_column(col, tree=tree))
        menu.add_separator()
        menu.add_command(label="Copy (TSV with headers)", command=self.copy_result_to_clipboard)
        menu.post(event.x_root, event.y_root)
    
    def _remember_clicked_column(self, event):
        tree = event.widget
        col_id = tree.identify_column(event.x)
        try:
            self._last_clicked_column_index = max(0, int(col_id.replace("#", "")) - 1)
        except Exception:
            self._last_clicked_column_index = 0
    
    def _copy_cell(self, tree=None):
        tree = tree or (self._get_active_result_tree()[0])
        if not tree: return
        sel = tree.selection()
        if not sel: return
        iid = sel[0]
        vals = tree.item(iid, "values")
        idx = self._last_clicked_column_index if self._last_clicked_column_index < len(vals) else 0
        val = vals[idx] if vals else ""
        self.clipboard_clear(); self.clipboard_append(str(val)); self.update()
        self.status_var.set("Cell copied")
    
    def _copy_row(self, iid, tree=None):
        tree = tree or (self._get_active_result_tree()[0])
        if not tree or not iid: return
        vals = tree.item(iid, "values")
        txt = "\t".join(map(str, vals))
        self.clipboard_clear(); self.clipboard_append(txt); self.update()
        self.status_var.set("Row copied")
    
    def _copy_column(self, col, tree=None):
        tree = tree or (self._get_active_result_tree()[0])
        if not tree: return
        try:
            clicked_idx = int(col.replace("#","")) - 1
        except Exception:
            return
    
        frm = self.res_nb.select(); st = self._res_tabs.get(frm)
        if not st or st["df_cur"] is None or st["df_cur"].empty: return
    
        if clicked_idx == 0:
            name = "#"
            series = pd.Series(range(1, len(st["df_cur"]) + 1))
        else:
            real_idx = clicked_idx - 1
            cols = list(st["df_cur"].columns)
            if real_idx < 0 or real_idx >= len(cols):
                return
            name = cols[real_idx]
            series = self._normalize_for_grid(st["df_cur"])[name]
    
        txt = name + "\n" + "\n".join("" if (v is None or (isinstance(v, float) and pd.isna(v))) else str(v) for v in series.tolist())
        self.clipboard_clear(); self.clipboard_append(txt); self.update()
        self.status_var.set(f"Column '{name}' copied")

    def export_result_csv(self):
        tree, st = self._get_active_result_tree()
        if not st or st["df_cur"] is None or st["df_cur"].empty:
            messagebox.showinfo("Export","No result."); return
        fp = filedialog.asksaveasfilename(title="Save CSV", defaultextension=".csv", filetypes=[("CSV",".csv")])
        if not fp: return
        st["df_cur"].to_csv(fp, index=False, encoding="utf-8-sig")
        messagebox.showinfo("Export", f"Saved: {fp}")
    
    def export_result_xlsx(self):
        tree, st = self._get_active_result_tree()
        if not st or st["df_cur"] is None or st["df_cur"].empty:
            messagebox.showinfo("Export","No result."); return
        if openpyxl is None: 
            messagebox.showerror("Export","openpyxl required"); return
        fp = filedialog.asksaveasfilename(title="Save Excel", defaultextension=".xlsx", filetypes=[("Excel",".xlsx")])
        if not fp: return
        with pd.ExcelWriter(fp, engine="openpyxl") as w:
            st["df_cur"].to_excel(w, index=False, sheet_name="Result")
        messagebox.showinfo("Export", f"Saved: {fp}")
    
    def export_result_json(self):
        tree, st = self._get_active_result_tree()
        if not st or st["df_cur"] is None or st["df_cur"].empty:
            messagebox.showinfo("Export","No result."); return
        fp = filedialog.asksaveasfilename(title="Save JSON", defaultextension=".json", filetypes=[("JSON",".json")])
        if not fp: return
        st["df_cur"].to_json(fp, orient="records", force_ascii=False)
        messagebox.showinfo("Export", f"Saved: {fp}")
    
    def copy_result_to_clipboard(self):
        tree, st = self._get_active_result_tree()
        if not st or st["df_cur"] is None or st["df_cur"].empty: return
        text = st["df_cur"].to_csv(sep="\t", index=False)
        self.clipboard_clear(); self.clipboard_append(text); self.update()
        self.status_var.set("Result copied to clipboard (TSV)")

    def _split_sql(self, sql: str):
        stmts, buf = [], []
        in_s=in_d=False; esc=False; i=0
        def flush():
            s = "".join(buf).strip()
            if s: stmts.append(s); buf.clear()
        while i<len(sql):
            if not in_s and not in_d:
                m = re.match(r"(?m)^\s*GO\s*(--.*)?$", sql[i:])
                if m:
                    flush(); nl = sql.find("\n", i); i = len(sql) if nl==-1 else nl+1; continue
            ch = sql[i]
            if ch=="\\" and not esc: esc=True; buf.append(ch); i+=1; continue
            if ch=="'" and not in_d and not esc: in_s = not in_s
            elif ch=='"' and not in_s and not esc: in_d = not in_d
            esc=False
            if ch==";" and not in_s and not in_d: flush()
            else: buf.append(ch)
            i+=1
        if buf: flush()
        return stmts

    def _current_stmt(self) -> str:
        txt = self.editor.get("1.0", tk.END); idx = self.editor.index(tk.INSERT)
        parts = self._split_sql(txt)
        cur_abs = int(self.editor.count("1.0", idx, "chars")[0])
        pos = 0
        for part in parts:
            seg = part + ";"; start, end = pos, pos+len(seg)
            if start <= cur_abs <= end: return part
            pos = end
        return txt
    
    def _get_selection_or_current_stmt(self) -> str:
        """If a selection exists, return it; otherwise return the current statement at the cursor."""
        try:
            sel = self.editor.get("sel.first", "sel.last")
            if sel and sel.strip():
                return sel
        except tk.TclError:
            pass
        return self._current_stmt()
    
    def run_current_stmt(self):
        sql = (self._get_selection_or_current_stmt() or "").strip()
        if not sql:
            return
        self._execute_sql(sql, open_result_tab_per_stmt=True)
        try:
            self.editor.get("sel.first", "sel.last")
            self.status_var.set("Ran selection (Ctrl+Enter)")
        except tk.TclError:
            self.status_var.set("Ran current statement (Ctrl+Enter)")

    def run_query(self):
        sql = self.editor.get("1.0", tk.END).strip()
        if not sql: return
        self._execute_sql(sql, open_result_tab_per_stmt=True)

    def _execute_sql(self, sql: str, open_result_tab_per_stmt: bool = True):
        t0 = time.time(); last_df=None; wrote=False; did_ctas=False
        self.msgbox.delete("1.0", tk.END)
        try:
            stmts = self._split_sql(sql)
            schema_tables = {}
            for (sch, internal), _ in self.catalog.registry.items():
                disp = self.catalog.names.to_display(sch, internal) or internal
                schema_tables.setdefault(sch, []).append(disp)
            base_title = self._get_active_editor_title()
            select_idx = 0
            for stmt in stmts:
                if not stmt.strip(): 
                    continue
                stmt = self.catalog.names.rewrite_sql(stmt, schema_tables)
                path = self.catalog.maybe_ctas(stmt)
                if path:
                    did_ctas = True
                    self._messages_write(f"CTAS → CSV saved:\n{path}\n")
                    continue
                if self.catalog.maybe_write_back(stmt):
                    wrote=True; self._messages_write("Write-back done.\n"); continue
                df = self.catalog.run_query_limited(stmt, None)
                last_df = df
                if open_result_tab_per_stmt:
                    select_idx += 1
                    tab_title = base_title if len(stmts) == 1 else f"{base_title} · {select_idx}"
                    self.results_show_dataframe(df, title=tab_title)
            if last_df is None and not open_result_tab_per_stmt:
                self.results_show_dataframe(pd.DataFrame(), title="Result")
                self.status_var.set(f"Done in {(time.time()-t0)*1000:.1f} ms (no rows)")
            else:
                self.status_var.set(f"Done — {(time.time()-t0)*1000:.1f} ms")
                self._messages_write(self.status_var.get() + "\n")
            if wrote or did_ctas:
                self.refresh_catalog()
        except PermissionError:
            messagebox.showerror("Query Error","Close the file in Excel and try again.")
            self._messages_write("ERROR: Permission denied\n")
        except Exception as e:
            messagebox.showerror("Query Error", str(e))
            self._messages_write(f"ERROR: {e}\n")

    def _messages_write(self, text: str):
        self.msgbox.insert(tk.END, text); self.msgbox.see(tk.END)

    def _on_text_modified(self, e=None):
        if self.editor.edit_modified():
            self._set_dirty(True); self.editor.edit_modified(False); self._update_line_numbers()

    def _set_dirty(self, dirty: bool):
        self._text_dirty = dirty
        title = APP_TITLE + (" *" if dirty else "")
        try: self.wm_title(title)
        except Exception: pass

    def _ask_string(self, title: str, prompt: str) -> str|None:
        d = tk.Toplevel(self); d.title(title)
        ttk.Label(d, text=prompt).pack(padx=10, pady=10)
        var = tk.StringVar(); e = ttk.Entry(d, textvariable=var, width=60); e.pack(padx=10, pady=5); e.focus_set()
        ans = {"v":None}
        def ok(): ans["v"]=var.get().strip(); d.destroy()
        def cancel(): ans["v"]=None; d.destroy()
        bb = ttk.Frame(d); bb.pack(pady=10)
        ttk.Button(bb, text="OK", command=ok).pack(side=tk.LEFT, padx=5)
        ttk.Button(bb, text="Cancel", command=cancel).pack(side=tk.LEFT, padx=5)
        d.grab_set(); d.wait_window(); return ans["v"]

    def _init_sql_highlighting(self, tab: EditorTab|None=None):
        txt = (tab.text if isinstance(tab, EditorTab) else self.editor)
        if not txt: return
        txt.tag_configure("kw", foreground="#005cc5")
        txt.tag_configure("fn", foreground="#6f42c1")
        txt.tag_configure("str", foreground="#b07d00")
        txt.tag_configure("com", foreground="#6a737d")
        txt.tag_configure("num", foreground="#22863a")
        txt.tag_configure("find_all", background="#fff4b1")
        txt.tag_configure("find_cur", background="#ffe187")
    
    def _apply_sql_highlighting(self, tab: EditorTab|None=None):
        txt = (tab.text if isinstance(tab, EditorTab) else self.editor)
        if not txt: return
        text = txt.get("1.0", tk.END)
        for t in ("kw","fn","str","com","num"):
            txt.tag_remove(t, "1.0", tk.END)
        if not text.strip(): return
        for m in re.finditer(r"--.*?$", text, re.M): self._tag_range(txt, m.start(), m.end(), "com")
        for m in re.finditer(r"'(?:''|[^'])*'", text): self._tag_range(txt, m.start(), m.end(), "str")
        for m in re.finditer(r"\b\d+(?:\.\d+)?\b", text): self._tag_range(txt, m.start(), m.end(), "num")
        kw=r"\b(SELECT|FROM|WHERE|GROUP|BY|ORDER|LIMIT|OFFSET|JOIN|LEFT|RIGHT|FULL|OUTER|INNER|ON|AND|OR|NOT|IN|IS|NULL|AS|CASE|WHEN|THEN|ELSE|END|WITH|UNION|ALL|INSERT|INTO|VALUES|UPDATE|SET|DELETE|CREATE|TABLE|VIEW|SCHEMA|DROP|DESCRIBE|EXPLAIN)\b"
        fn=r"\b(COUNT|SUM|AVG|MIN|MAX|COALESCE|ROUND|CAST|DATE|YEAR|MONTH|DAY|LOWER|UPPER|LENGTH|SUBSTRING|REGEXP_MATCHES)\b"
        for m in re.finditer(kw, text, re.I): self._tag_range(txt, m.start(), m.end(), "kw")
        for m in re.finditer(fn, text, re.I): self._tag_range(txt, m.start(), m.end(), "fn")
    
    def _tag_range(self, txt: tk.Text, sidx, eidx, tag):
        text = txt.get("1.0", tk.END)
        line = text.count("\n", 0, sidx) + 1
        col = sidx - (text.rfind("\n", 0, sidx) + 1 if text.rfind("\n", 0, sidx)!=-1 else 0)
        s = f"{line}.{col}"
        line2 = text.count("\n", 0, eidx) + 1
        col2 = eidx - (text.rfind("\n", 0, eidx) + 1 if text.rfind("\n", 0, eidx)!=-1 else 0)
        e = f"{line2}.{col2}"
        txt.tag_add(tag, s, e)

    def _update_line_numbers(self):
        self.linenum.delete("all")
        text = self.editor.get("1.0", tk.END)
        lines = text.count("\n") + 1
        for i in range(1, lines+1):
            y = self.editor.dlineinfo(f"{i}.0")
            if y: self.linenum.create_text(2, y[1], anchor="nw", text=str(i), fill="#999")

    def toggle_comment(self):
        try:
            start = self.editor.index("sel.first"); end = self.editor.index("sel.last")
        except tk.TclError:
            cur = self.editor.index(tk.INSERT)
            line_start = f"{cur.split('.')[0]}.0"; line_end = f"{int(cur.split('.')[0])}.end"
            text = self.editor.get(line_start, line_end)
            new = re.sub(r"^\s*--\s?", "", text) if text.lstrip().startswith("--") else "-- " + text
            self.editor.delete(line_start, line_end); self.editor.insert(line_start, new); self._apply_sql_highlighting(); self._update_line_numbers(); return
        lines = self.editor.get(start, end).splitlines()
        cnt = sum(1 for ln in lines if ln.lstrip().startswith("--"))
        mode = cnt < len(lines)/2
        new_lines = [("-- "+ln) if mode else re.sub(r"^\s*--\s?","", ln) for ln in lines]
        self.editor.delete(start, end); self.editor.insert(start, "\n".join(new_lines))
        self._apply_sql_highlighting(); self._update_line_numbers()

    def find_in_editor(self):
        if getattr(self, "_find_win", None) and self._find_win.winfo_exists():
            self._find_win.deiconify(); self._find_entry.focus_set(); return
    
        self._find_wrap  = tk.BooleanVar(value=True)
        self._find_case  = tk.BooleanVar(value=False)
        self._find_regex = tk.BooleanVar(value=False)
        self._find_term  = tk.StringVar(value="")
        self._replace_term = tk.StringVar(value="")
        self._find_last_index = "1.0"
        self._find_last_term  = ""
    
        win = self._find_win = tk.Toplevel(self)
        win.title("Find / Replace"); win.transient(self); win.resizable(False, False)
        win.geometry(f"+{self.winfo_rootx()+120}+{self.winfo_rooty()+120}")
    
        frm = ttk.Frame(win); frm.pack(padx=8, pady=8)
    
        ttk.Label(frm, text="Find:").grid(row=0, column=0, padx=(0,6), pady=(0,4))
        ent = ttk.Entry(frm, textvariable=self._find_term, width=42)
        ent.grid(row=0, column=1, columnspan=6, sticky="ew", pady=(0,4))
        self._find_entry = ent
    
        ttk.Label(frm, text="Replace:").grid(row=1, column=0, padx=(0,6))
        rep = ttk.Entry(frm, textvariable=self._replace_term, width=42)
        rep.grid(row=1, column=1, columnspan=6, sticky="ew")
    
        ttk.Checkbutton(frm, text="Match case", variable=self._find_case).grid(row=2, column=1, sticky="w", pady=(6,0))
        ttk.Checkbutton(frm, text="Regex",      variable=self._find_regex).grid(row=2, column=2, sticky="w", pady=(6,0))
        ttk.Checkbutton(frm, text="Wrap",       variable=self._find_wrap).grid(row=2, column=3, sticky="w", pady=(6,0))
    
        ttk.Button(frm, text="Next (F3)",      command=self._find_next).grid(row=0, column=7, padx=(8,0))
        ttk.Button(frm, text="Prev (Shift+F3)",command=self._find_prev).grid(row=1, column=7, padx=(8,0))
        ttk.Button(frm, text="Replace",        command=self._replace_one).grid(row=2, column=6, padx=(8,0), pady=(6,0))
        ttk.Button(frm, text="Replace All",    command=self._replace_all).grid(row=2, column=7, padx=(6,0), pady=(6,0))
        ttk.Button(frm, text="Close",          command=win.withdraw).grid(row=2, column=5, padx=(6,0), pady=(6,0))
    
        ent.bind("<Return>", lambda e: self._find_next())
        rep.bind("<Return>", lambda e: self._replace_one())
        win.bind("<Escape>", lambda e: win.withdraw())
        ent.bind("<KeyRelease>", lambda e: self._find_highlight_all())
    
        try:
            sel = self.editor.get("sel.first", "sel.last")
            if sel.strip(): self._find_term.set(sel)
        except tk.TclError:
            pass
    
        self._find_highlight_all()
        ent.focus_set()

    def _find_opts(self):
        nocase = 0 if self._find_case.get() else 1
        regexp = True if self._find_regex.get() else False
        return {"nocase": nocase, "regexp": regexp}
    
    def _find_highlight_all(self):
        term = self._find_term.get()
        self.editor.tag_remove("find_all", "1.0", tk.END)
        self.editor.tag_remove("find_cur", "1.0", tk.END)
        if not term:
            self.status_var.set("Ready."); return
        start = "1.0"; count_var = tk.IntVar(); n = 0
        while True:
            idx = self.editor.search(term, start, stopindex=tk.END, count=count_var, **self._find_opts())
            if not idx: break
            end = self.editor.index(f"{idx}+{count_var.get()}c")
            self.editor.tag_add("find_all", idx, end)
            n += 1; start = end
        self.status_var.set(f"Found {n} match(es).")
        self._find_last_index = "1.0"; self._find_last_term = term
    
    def _find_jump_to(self, idx, length):
        end = self.editor.index(f"{idx}+{length}c")
        self.editor.tag_remove("find_cur", "1.0", tk.END)
        self.editor.tag_add("find_cur", idx, end)
        self.editor.tag_remove("sel", "1.0", tk.END)
        self.editor.tag_add("sel", idx, end)
        self.editor.mark_set(tk.INSERT, end)
        self.editor.see(idx)
    
    def _find_next(self):
        term = self._find_term.get()
        if not term: self.status_var.set("Type something to find."); return
        if term != getattr(self, "_find_last_term", ""): self._find_highlight_all()
        try: start = self.editor.index("sel.last")
        except tk.TclError: start = self.editor.index(tk.INSERT)
        count_var = tk.IntVar()
        idx = self.editor.search(term, start, stopindex=tk.END, count=count_var, **self._find_opts())
        if not idx and self._find_wrap.get():
            idx = self.editor.search(term, "1.0", stopindex=tk.END, count=count_var, **self._find_opts())
        if not idx: self.status_var.set("No more matches."); return
        self._find_jump_to(idx, count_var.get())
    
    def _find_prev(self):
        term = self._find_term.get()
        if not term: self.status_var.set("Type something to find."); return
        try: cur = self.editor.index("sel.first")
        except tk.TclError: cur = self.editor.index(tk.INSERT)
        count_var = tk.IntVar(); last_idx, last_len = None, 0; start = "1.0"
        while True:
            idx = self.editor.search(term, start, stopindex=cur, count=count_var, **self._find_opts())
            if not idx: break
            last_idx, last_len = idx, count_var.get()
            start = self.editor.index(f"{idx}+{count_var.get()}c")
        if not last_idx and self._find_wrap.get():
            start = "1.0"; cur = tk.END
            while True:
                idx = self.editor.search(term, start, stopindex=cur, count=count_var, **self._find_opts())
                if not idx: break
                last_idx, last_len = idx, count_var.get()
                start = self.editor.index(f"{idx}+{count_var.get()}c")
        if not last_idx: self.status_var.set("No previous matches."); return
        self._find_jump_to(last_idx, last_len)

    def _current_match_range(self):
        """Returns (start, end) for the current match if any, else None."""
        rng = self.editor.tag_ranges("find_cur")
        if rng:
            return str(rng[0]), str(rng[1])
        try:
            s = self.editor.index("sel.first"); e = self.editor.index("sel.last")
            return s, e
        except tk.TclError:
            return None
    
    def _compute_replacement_text(self, matched_text: str):
        """Computes replacement text according to options."""
        repl = self._replace_term.get()
        term = self._find_term.get()
        if self._find_regex.get():
            flags = 0 if self._find_case.get() else re.IGNORECASE
            try:
                return re.sub(term, repl, matched_text, count=1, flags=flags)
            except re.error as e:
                messagebox.showerror("Regex Error", f"{e}")
                return matched_text
        else:
            return repl
    
    def _replace_one(self):
        if not self._find_term.get():
            self.status_var.set("Type something to find."); return
        rng = self._current_match_range()
        if not rng:
            self._find_next()
            rng = self._current_match_range()
            if not rng:
                self.status_var.set("No match to replace."); return
        s, e = rng
        matched = self.editor.get(s, e)
        newtxt = self._compute_replacement_text(matched)
        self.editor.edit_separator()
        self.editor.delete(s, e)
        self.editor.insert(s, newtxt)
        self.editor.edit_separator()
        self._find_highlight_all()
        after = self.editor.index(f"{s}+{len(newtxt)}c")
        self.editor.mark_set(tk.INSERT, after)
        self._find_next()
        self.status_var.set("Replaced.")
    
    def _replace_all(self):
        term = self._find_term.get()
        if not term:
            self.status_var.set("Type something to find."); return
    
        start = "1.0"
        count_var = tk.IntVar()
        replaced = 0
    
        self.editor.edit_separator()
        while True:
            idx = self.editor.search(term, start, stopindex=tk.END, count=count_var, **self._find_opts())
            if not idx:
                break
            end = self.editor.index(f"{idx}+{count_var.get()}c")
            matched = self.editor.get(idx, end)
            newtxt = self._compute_replacement_text(matched)
    
            self.editor.delete(idx, end)
            self.editor.insert(idx, newtxt)
            replaced += 1
            start = self.editor.index(f"{idx}+{len(newtxt)}c")
        self.editor.edit_separator()
    
        self._find_highlight_all()
        self.status_var.set(f"Replaced {replaced} occurrence(s).")

    def clear_query(self):
        cur = self.ed_nb.select(); tab = self._editor_tabs.get(cur)
        if not tab: return
        tab.text.delete("1.0", tk.END)
        self._apply_sql_highlighting(tab); tab.update_linenum()

    def insert_examples(self):
        ex = (
            "\n-- Examples\n"
            "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema='root';\n"
            "SELECT * FROM root.customers LIMIT 100;\n"
            "DESCRIBE root.customers;\n"
        )
        self.editor.insert(tk.END, ex); self._apply_sql_highlighting(); self._update_line_numbers()

    def _toggle_dark(self):
        self._dark = not self._dark
        if self._dark: self._apply_dark()
        else: self._apply_light()

    def _apply_dark(self):
        bg="#111417"; fg="#e6e6e6"; panel="#1a1f24"; hdr="#20262c"
        self.configure(bg=bg)
        self.option_add("*Menu*background", panel)
        self.option_add("*Menu*foreground", fg)
        self.editor.configure(bg=panel, fg=fg, insertbackground=fg)
        self.linenum.configure(background="#0f1418")
        self.msgbox.configure(bg=panel, fg=fg, insertbackground=fg)
        style = ttk.Style(self)
        for tab_id in self.ed_nb.tabs():
            tab = self._editor_tabs.get(tab_id)
            if tab:
                tab.text.configure(bg="#1a1f24", fg="#e6e6e6", insertbackground="#e6e6e6")
                tab.linenum.configure(background="#0f1418")

        try: style.theme_use("clam")
        except Exception: pass
        for sty in ("Treeview","TFrame","TLabel","TButton","TNotebook","TNotebook.Tab"):
            try:
                if sty=="Treeview":
                    style.configure(sty, background=panel, fieldbackground=panel, foreground=fg, bordercolor=panel)
                    style.configure("Treeview.Heading", background=hdr, foreground=fg)
                elif sty=="TFrame":
                    style.configure(sty, background=bg)
                elif sty in ("TLabel","TButton"):
                    style.configure(sty, background=panel if sty=="TButton" else bg, foreground=fg)
                elif sty=="TNotebook":
                    style.configure(sty, background=bg)
                elif sty=="TNotebook.Tab":
                    style.configure(sty, background=hdr, foreground=fg)
            except Exception:
                pass

    def _apply_light(self):
        self.configure(bg="")
        style = ttk.Style(self)
        for tab_id in self.ed_nb.tabs():
            tab = self._editor_tabs.get(tab_id)
            if tab:
                tab.text.configure(bg="white", fg="black", insertbackground="black")
                tab.linenum.configure(background="#f5f5f5")

        try: style.theme_use("vista" if sys.platform.startswith("win") else "default")
        except Exception: pass
        self.editor.configure(bg="white", fg="black", insertbackground="black")
        self.msgbox.configure(bg="white", fg="black", insertbackground="black")
        self.linenum.configure(background="#f5f5f5")

    def profile_dialog(self, table_name: str | None = None):
        tree, st = self._get_active_result_tree()
        if not st or st["df_cur"] is None or st["df_cur"].empty:
            messagebox.showinfo("Profiler", "No data loaded to profile.")
            return
        df = st["df_cur"].copy()
        try:
            desc = df.describe(include="all", datetime_is_numeric=True).T
        except TypeError:
            for c in df.columns:
                try:
                    if pd.api.types.is_datetime64_any_dtype(df[c]):
                        df[c] = pd.to_datetime(df[c], errors="coerce").view("int64") // 10**9
                except Exception:
                    pass
            desc = df.describe(include="all").T
        self.results_show_dataframe(desc, title="Profile")
        self.nb.select(self.tab_results)
        self.status_var.set(f"Profile generated for: {table_name or 'Result'}")

    def undo_last_write(self):
        if not self.servers:
            messagebox.showinfo("Undo", "Connect to at least one Server first.")
            return
        latest_bak = None
        latest_time = 0
        for srv in self.servers:
            for root, _, files in os.walk(srv):
                for f in files:
                    if f.endswith(".bak"):
                        path = Path(root) / f
                        t = path.stat().st_mtime
                        if t > latest_time:
                            latest_time = t; latest_bak = path
        if not latest_bak:
            messagebox.showinfo("Undo", "No .bak files found.")
            return
        orig = latest_bak.with_suffix("")
        try:
            os.replace(latest_bak, orig)
            messagebox.showinfo("Undo", f"Restored backup:\n{orig.name}")
            self.refresh_catalog()
        except Exception as e:
            messagebox.showerror("Undo Error", str(e))

    def _create_result_tab(self, title: str):
        frm = ttk.Frame(self.res_nb)
        frm.rowconfigure(0, weight=1); frm.columnconfigure(0, weight=1)
        tree = ttk.Treeview(frm, show="headings", selectmode="extended")
        g_v = ttk.Scrollbar(frm, orient="vertical", command=tree.yview)
        g_h = ttk.Scrollbar(frm, orient="horizontal", command=tree.xview)
        tree.grid(row=0, column=0, sticky="nsew"); g_v.grid(row=0, column=1, sticky="ns"); g_h.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=g_v.set, xscrollcommand=g_h.set)
        tree.bind("<Button-1>", self._remember_clicked_column, add=True)
        tree.bind("<Double-1>", lambda e, t=tree: self._copy_cell(tree=t))
        tree.bind("<Button-3>", lambda e, t=tree: self._grid_context(e, tree=t))
        self.res_nb.add(frm, text=title)
        self._res_tabs[str(frm)] = {"tree": tree, "df_orig": None, "df_cur": None, "sort": {}}
        self.res_nb.select(frm)
        return frm

    def _editor_tab_menu(self, event):
        x, y = event.x, event.y
        for i, tab_id in enumerate(self.ed_nb.tabs()):
            bbox = self.ed_nb.bbox(i)
            if bbox and (bbox[0] <= x <= bbox[0]+bbox[2]) and (bbox[1] <= y <= bbox[1]+bbox[3]):
                self.ed_nb.select(tab_id); break
        m = tk.Menu(self, tearoff=False)
        m.add_command(label="New Query (Ctrl+T)", command=lambda: self._new_editor_tab())
        m.add_command(label="Close (Ctrl+W)", command=lambda: self._close_editor_tab())
        m.post(event.x_root, event.y_root)

    def _on_result_tab_changed(self):
        frm = self.res_nb.select()
        state = self._res_tabs.get(frm)
        if not state: 
            for b in (self.btn_csv,self.btn_xlsx,self.btn_json,self.btn_copy,self.btn_prof):
                b.config(state=tk.DISABLED)
            return
        has = state["df_cur"] is not None and not state["df_cur"].empty
        for b in (self.btn_csv,self.btn_xlsx,self.btn_json,self.btn_copy,self.btn_prof):
            b.config(state=(tk.NORMAL if has else tk.DISABLED))
    
    def _get_active_result_tree(self):
        frm = self.res_nb.select()
        st = self._res_tabs.get(frm)
        return (st["tree"], st) if st else (None, None)
    
    def _result_tab_menu(self, event):
        x = event.x; y = event.y
        for i, tab_id in enumerate(self.res_nb.tabs()):
            bbox = self.res_nb.bbox(i)
            if bbox and (bbox[0] <= x <= bbox[0]+bbox[2]) and (bbox[1] <= y <= bbox[1]+bbox[3]):
                self.res_nb.select(tab_id)
                break
        m = tk.Menu(self, tearoff=False)
        m.add_command(label="Close", command=lambda: self._result_close_current())
        m.add_command(label="Close Others", command=lambda: self._result_close_others())
        m.add_command(label="Close All", command=lambda: self._result_close_all())
        m.post(event.x_root, event.y_root)
    
    def _result_close_current(self):
        cur = self.res_nb.select()
        if cur: 
            self.res_nb.forget(cur); self._res_tabs.pop(cur, None)
    
    def _result_close_others(self):
        cur = self.res_nb.select()
        for t in list(self.res_nb.tabs()):
            if t != cur:
                self.res_nb.forget(t); self._res_tabs.pop(t, None)
    
    def _result_close_all(self):
        for t in list(self.res_nb.tabs()):
            self.res_nb.forget(t)
        self._res_tabs.clear()
        self._on_result_tab_changed()
class _ACPopup(tk.Toplevel):
    def __init__(self, master, on_commit):
        super().__init__(master)
        self.withdraw(); self.overrideredirect(True)
        self.list = tk.Listbox(self, activestyle="dotbox", height=10)
        self.list.pack(fill=tk.BOTH, expand=True)
        self.on_commit = on_commit
        self.list.bind("<Return>", lambda e: self._commit())
        self.list.bind("<Tab>", lambda e: self._commit())
        self.list.bind("<Escape>", lambda e: self.hide())
        self.list.bind("<Down>", lambda e: (self._move(1), "break")[1])
        self.list.bind("<Up>",   lambda e: (self._move(-1), "break")[1])
        self.list.bind("<Double-Button-1>", lambda e: self._commit())
        self.bind("<FocusOut>", lambda e: self.hide())
        self._items = []

    def _move(self, delta):
        if not self.list.size(): return
        cur = self.list.curselection()
        i = (cur[0] if cur else 0) + delta
        i = min(max(i, 0), self.list.size() - 1)
        self.list.selection_clear(0, tk.END)
        self.list.selection_set(i); self.list.activate(i); self.list.see(i)

    def _commit(self):
        sel = self.list.curselection()
        if not sel: return
        val = self.list.get(sel[0])
        self.on_commit(val)
        self.hide()

    def show(self, x, y, items):
        self.list.delete(0, tk.END)
        self._items = items
        for it in items[:400]:
            self.list.insert(tk.END, it)
        if not items:
            self.withdraw(); return
        self.list.selection_clear(0, tk.END)
        self.list.selection_set(0); self.list.activate(0)
        self.geometry(f"+{x}+{y}")
        self.deiconify(); self.lift(); self.focus_force()

    def hide(self): self.withdraw()
    def is_visible(self): return bool(self.state() == "normal")


def _token_at_cursor(txt: tk.Text):
    idx = txt.index(tk.INSERT)
    line = txt.get(f"{idx} linestart", f"{idx} lineend")
    col = int(idx.split(".")[1])
    L = col
    while L > 0 and re.match(r"[A-Za-z0-9_\.]", line[L-1]): L -= 1
    R = col
    while R < len(line) and re.match(r"[A-Za-z0-9_\.]", line[R:R+1]): R += 1
    return line[L:R], L, R

_SQL_KW = [
    "SELECT","FROM","WHERE","GROUP","BY","ORDER","LIMIT","JOIN","ON","AND","OR","NOT","IN","IS","NULL","AS",
    "CASE","WHEN","THEN","ELSE","END","WITH","UNION","ALL","INSERT","INTO","VALUES","UPDATE","SET","DELETE",
    "CREATE","TABLE","VIEW","SCHEMA","DROP","DESCRIBE","EXPLAIN"
]

def _strip_quotes(obj: str) -> str:
    if not obj:
        return obj
    if (obj.startswith('"') and obj.endswith('"')) or (obj.startswith('[') and obj.endswith(']')):
        return obj[1:-1]
    return obj

def _quote_ident_for_sql(s: str) -> str:
    """Adds quotes around an identifier if not already quoted, escaping inner quotes."""
    if not s or s == "*":
        return s
    if (s.startswith('"') and s.endswith('"')) or (s.startswith('[') and s.endswith(']')):
        return s
    return '"' + s.replace('"', '""') + '"'


def _extract_aliases(sql: str) -> dict:
    """
    Captures FROM/JOIN <table> [AS] <alias> even if table contains spaces,
    [] brackets, "" quotes, dashes, or non-Latin characters.
    """
    aliases = {}
    pat = r'(?is)\b(?:FROM|JOIN)\s+(.+?)\s+(?:AS\s+)?([A-Za-z_]\w*)\b'
    for m in re.finditer(pat, sql):
        raw_tbl = m.group(1).strip()
        ali     = m.group(2)
        raw_tbl = re.sub(r'\s*,\s*$', '', raw_tbl)
        raw_tbl = re.sub(r'\s*\.\s*', '.', raw_tbl)
        parts = [ _strip_quotes(p.strip()) for p in raw_tbl.split('.') if p.strip() ]
        tbl = '.'.join(parts) if parts else raw_tbl
        aliases[ali] = tbl
    return aliases


def _resolve_table_display(full: str, known_tables: list[str]) -> str:
    def _norm(s: str) -> str:
        s = s.replace('"', '').replace('[', '').replace(']', '')
        s = re.sub(r'\s*\.\s*', '.', s)
        return s.strip()

    nf = _norm(full)
    for kt in known_tables:
        if _norm(kt) == nf:
            return kt
    return full



def attach_autocomplete(app: App):
    app._ac = _ACPopup(app, on_commit=lambda val: _ac_commit(app, val))
    app.bind("<Control-space>", lambda e: _ac_trigger(app))
    app.editor.bind("<KeyRelease-period>", lambda e: _ac_trigger(app), add=True)

    def _ac_visible(): return hasattr(app, "_ac") and app._ac.is_visible()
    app.editor.bind("<Up>",     lambda e: (app._ac.list.event_generate("<Up>"),     "break")[1] if _ac_visible() else None, add=True)
    app.editor.bind("<Down>",   lambda e: (app._ac.list.event_generate("<Down>"),   "break")[1] if _ac_visible() else None, add=True)
    app.editor.bind("<Return>", lambda e: (app._ac.list.event_generate("<Return>"), "break")[1] if _ac_visible() else None, add=True)
    app.editor.bind("<Tab>",    lambda e: (app._ac.list.event_generate("<Tab>"),    "break")[1] if _ac_visible() else None, add=True)
    app.editor.bind("<Button-1>", lambda e: app._ac.hide(), add=True)
    app.editor.bind("<Key>",      lambda e: app._ac.hide(), add=True)

def _collect_catalog(app: App):
    schemas = sorted(set(app.catalog.schemas.keys()))
    tables_disp = []
    cols_by_table = {}
    for (sch, internal), _ in app.catalog.registry.items():
        disp = app.catalog.names.to_display(sch, internal) or internal
        full = f"{sch}.{disp}"
        tables_disp.append(full)
        if full in app._catalog_cols_cache:
            cols_by_table[full] = app._catalog_cols_cache[full]
        else:
            try:
                ddesc = app.catalog.describe(sch, internal)
                field = "colname" if "colname" in ddesc.columns else ddesc.columns[0]
                cols = ddesc[field].astype(str).tolist()
            except Exception:
                cols = []
            app._catalog_cols_cache[full] = cols
            cols_by_table[full] = cols
    return schemas, sorted(set(tables_disp)), cols_by_table

def _ac_trigger(app: App):
    sql = app.editor.get("1.0", tk.END)
    aliases = _extract_aliases(sql)
    tok, L, R = _token_at_cursor(app.editor)

    schemas, tables, cols_by_table = _collect_catalog(app)
    tables_set = set(tables)

    all_cols = []
    for t in tables:
        for c in cols_by_table.get(t, []):
            all_cols.append(f"{t}.{c}")
    for ali, ref in aliases.items():
        ref_disp = _resolve_table_display(ref, tables)
        acols = cols_by_table.get(ref_disp, [])
        all_cols.extend([f"{ali}.{c}" for c in acols])

    items = []
    if "." in tok:
        parts = tok.split(".")
        if len(parts) == 1:
            items = schemas
        elif len(parts) == 2:
            prefix = parts[0]
            if prefix in aliases:
                items = [c for c in all_cols if c.startswith(prefix + ".")]
            else:
                items = [t for t in tables if t.startswith(prefix + ".")]
        elif len(parts) >= 3:
            pref = ".".join(parts[:2]) + "."
            items = [c for c in all_cols if c.startswith(pref)]
    else:
        items = _SQL_KW + schemas + tables + list(aliases.keys())

    bbox = app.editor.bbox(tk.INSERT)
    if not bbox: return
    x = app.editor.winfo_rootx() + bbox[0]
    y = app.editor.winfo_rooty() + bbox[1] + bbox[3]
    app._ac_span = (L, R)
    app._ac.show(x, y, sorted(set(items)))

def _ac_commit(app: App, value):
    if not value:
        return
    L, R = app._ac_span
    cur = app.editor.index(tk.INSERT)
    linestart = f"{cur} linestart"
    line = app.editor.get(linestart, f"{linestart} lineend")

    sql_all = app.editor.get("1.0", tk.END)
    schemas, tables, cols_by_table = _collect_catalog(app)
    aliases = set(_extract_aliases(sql_all).keys())

    if value.upper() in _SQL_KW:
        final = value
    else:
        parts = [p.strip() for p in re.split(r'\s*\.\s*', value) if p.strip()]
        if not parts:
            final = value
        else:
            out = []
            for i, tok in enumerate(parts):
                if i == 0 and (tok in schemas or tok in aliases):
                    out.append(tok)
                else:
                    out.append(_quote_ident_for_sql(tok))
            final = ".".join(out)

    before, after = line[:L], line[R:]
    newline = before + final + after
    app.editor.delete(linestart, f"{linestart} lineend")
    app.editor.insert(linestart, newline)
    app.editor.mark_set(tk.INSERT, f"{linestart}+{len(before)+len(final)}c")
    app._apply_sql_highlighting()


if __name__ == "__main__":
    app = App()
    attach_autocomplete(app)
    app.mainloop()
