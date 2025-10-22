import threading
import tkinter as tk
from tkinter import ttk, messagebox
from tkinter import font as tkfont
from typing import Optional, Tuple

import pandas as pd
from scipy import stats
import yaml
from sqlalchemy import create_engine

from queries import t_test_queries, chi_square_queries

# Load config.yaml for DB, tables, colors, and labels
CONFIG = {}
TABLES = {}
ENGINE = None
try:
    with open('config.yaml', 'r') as f:
        CONFIG = yaml.safe_load(f)
    TABLES = CONFIG['tables']
    ENGINE = create_engine(CONFIG['db_url'])
except Exception as e:
    messagebox.showerror("Config Error", f"Failed to load config.yaml or connect to DB: {e}")

# Extract constants from config.yaml (strictly, without defaults)
WHITE = CONFIG['colors']['white']
BLACK = CONFIG['colors']['black']
ACCENT = CONFIG['colors']['accent']
CONTROL_WIDTH_CHARS = CONFIG['control_width_chars']
EN_METRIC_LABELS = CONFIG['metric_labels']

# Font loading (tkextrafont)
HAS_EXTRA_FONT = False
try:
    from tkextrafont import Font as ExtraFont
    import os

    def load_local_fonts() -> None:
        global HAS_EXTRA_FONT
        base_dir = os.path.dirname(os.path.abspath(__file__))
        fonts_dir = os.path.join(base_dir, "fonts")
        try:
            for font_file, family in [
                ("Montserrat-Regular.ttf", "Montserrat"),
                ("Montserrat-MediumItalic.ttf", "Montserrat Medium Italic"),
                ("Montserrat-Bold.ttf", "Montserrat Bold"),
            ]:
                path = os.path.join(fonts_dir, font_file)
                if os.path.exists(path):
                    ExtraFont(file=path, family=family)
            HAS_EXTRA_FONT = True
        except Exception:
            HAS_EXTRA_FONT = False

except ImportError:
    def load_local_fonts() -> None:
        pass

def run_ttest_gui(metric_id: int, group_choice: int) -> Tuple[bool, str]:
    """Run t-test using queries from queries.py."""
    if ENGINE is None:
        return False, "Error: Failed to connect to DB. Check config.yaml."

    if metric_id not in t_test_queries:
        return False, f"Error: Unknown metric ID={metric_id}."

    test_info = t_test_queries[metric_id]
    metric_label_en = EN_METRIC_LABELS.get(metric_id, test_info['name'])

    sql_query = test_info["sql_male_female"] if group_choice == 1 else test_info["sql_web_mobile"]
    group1_name, group2_name = ("M", "F") if group_choice == 1 else ("WEB", "MOBILE")

    try:
        df_stats = pd.read_sql(sql_query, ENGINE)
        if len(df_stats) != 2:
            return False, f"Error: Expected 2 groups, got {len(df_stats)}. Groups: {df_stats['group_name'].tolist()}"

        df_stats["group_norm"] = df_stats["group_name"].str.strip().str.upper()
        group1_stats = df_stats[df_stats["group_norm"] == group1_name]
        group2_stats = df_stats[df_stats["group_norm"] == group2_name]

        if group1_stats.empty or group2_stats.empty:
            return False, f"Error: Missing data for {group1_name} or {group2_name}."

        mean1, std1, n1 = group1_stats.iloc[0][["mean", "stddev", "count"]]
        mean2, std2, n2 = group2_stats.iloc[0][["mean", "stddev", "count"]]

        t_stat, p_val = stats.ttest_ind_from_stats(
            mean1=float(mean1), std1=float(std1), nobs1=int(n1),
            mean2=float(mean2), std2=float(std2), nobs2=int(n2),
            equal_var=False
        )

        decision = "Significant difference" if p_val < 0.05 else "No statistically significant difference"
        result_text = (
            f"t-test — {metric_label_en}\n"
            f"Groups: {group1_name} vs {group2_name}\n"
            f"t-statistic: {t_stat:.4f}\n"
            f"p-value: {p_val:.4f}\n"
            f"Conclusion: {decision} (alpha=0.05)"
        )
        return True, result_text
    except Exception as e:
        return False, f"Error during t-test: {e}"

def run_chi_square_gui(group_choice: int) -> Tuple[bool, str]:
    """Run chi-square test using queries from queries.py."""
    if ENGINE is None:
        return False, "Error: Failed to connect to DB. Check config.yaml."

    if group_choice == 1:
        sql = chi_square_queries[1]['sql_male_female']
        group_col = "gender"
        group_title = "Gender"
    else:
        sql = chi_square_queries[2]['sql_web_mobile']
        group_col = "traffic_source"
        group_title = "Traffic source"

    try:
        df = pd.read_sql(sql, ENGINE)
        if df.empty:
            return False, "Error: Empty SQL result."

        contingency_table = pd.crosstab(df["payment_method"], df[group_col])
        contingency_table_percent = (pd.crosstab(df["payment_method"], df[group_col], normalize="columns") * 100).round(0)

        chi2_stat, p_value, dof, expected = stats.chi2_contingency(contingency_table)

        lines = [
            f"Does percentage of Payment_method differ across {group_title}? \n",
            "Number of customers:\n",
            contingency_table.to_string(),
            "\n Number of customers. % of totals by " + group_title + ":  \n",
            contingency_table_percent.to_string() + " \n",
            f"Null hypothesis: Payment_method distribution is independent of {group_title}.",
            f"P-value of Chi-square test = {round(float(p_value), 3)}",
        ]
        if p_value < 0.05:
            lines.extend([
                "We reject Null hypothesis. \n",
                f"There is statistical evidence that Payment_method distribution differs across {group_title}."
            ])
        else:
            lines.extend([
                "We fail to reject the null hypothesis \n",
                f"There is NO statistically significant evidence that Payment_method distribution differs across {group_title}."
            ])

        return True, "\n".join(lines)
    except Exception as e:
        return False, f"Error during Chi-square test: {e}"

def create_accent_dropdown(parent: tk.Widget, variable: tk.StringVar, values: list[str], font: tkfont.Font, width_chars: int) -> tk.OptionMenu:
    """Create styled dropdown menu."""
    if not values:
        values = [""]
    if variable.get() not in values:
        variable.set(values[0])
    om = tk.OptionMenu(parent, variable, *values)
    om.config(font=font, bg=ACCENT, fg=BLACK, activebackground=ACCENT, activeforeground=BLACK, highlightthickness=1, bd=1, width=width_chars)
    menu = om.nametowidget(om.menuname)
    menu.config(bg=ACCENT, fg=BLACK, activebackground=ACCENT, activeforeground=BLACK, font=font)
    return om

class StatTestsApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Statistical Tests")
        self.geometry("980x720")
        self.minsize(900, 620)
        self.configure(bg=WHITE)

        load_local_fonts()

        self._init_fonts()
        self._init_styles()

        self._loader_win: Optional[tk.Toplevel] = None

        self.container = tk.Frame(self, bg=WHITE)
        self.container.pack(fill=tk.BOTH, expand=True)

        self.main_menu = self._build_main_menu()
        self.ttest_view = self._build_ttest_view()
        self.chi2_view = self._build_chi2_view()

        self._show(self.main_menu)

        if ENGINE is None:
            messagebox.showwarning("Database", "Failed to connect to DB. Check config.yaml.")

    def _init_fonts(self) -> None:
        base = 16
        main_title = int(base * 2.25)
        test_title = int(base * 1.3)
        family_regular = "Montserrat" if HAS_EXTRA_FONT else "Arial"

        self.font_ui = tkfont.Font(family=family_regular, size=base)
        self.font_ui_bold = tkfont.Font(family=family_regular, size=base, weight="bold")
        self.font_dropdown = self.font_ui
        self.font_main_header = tkfont.Font(family=family_regular, size=main_title, weight="bold")
        self.font_test_header = tkfont.Font(family=family_regular, size=test_title, weight="bold")
        self.font_result = tkfont.Font(family=family_regular, size=base - 2)
        self.font_result_bold = tkfont.Font(family=family_regular, size=base - 2, weight="bold")

    def _init_styles(self) -> None:
        style = ttk.Style(self)
        style.theme_use("clam")

        style.configure("Accent.TButton", font=self.font_ui, padding=(20, 12), background=ACCENT, foreground=BLACK, borderwidth=1)
        style.map("Accent.TButton", background=[("active", ACCENT)], foreground=[("active", BLACK)])

        style.configure("Action.TButton", font=self.font_ui, padding=(20, 8), background=ACCENT, foreground=BLACK, borderwidth=1)
        style.map("Action.TButton", background=[("active", ACCENT)], foreground=[("active", BLACK)])

        style.configure("Accent.Vertical.TScrollbar", background=ACCENT, troughcolor=WHITE)
        style.configure("Accent.Horizontal.TProgressbar", troughcolor=WHITE, background=ACCENT)

    def _show(self, frame: tk.Frame) -> None:
        for child in self.container.winfo_children():
            child.pack_forget()
        frame.pack(fill=tk.BOTH, expand=True)

    def _build_main_menu(self) -> tk.Frame:
        frame = tk.Frame(self.container, bg=WHITE)

        tk.Label(frame, text="Statistical Tests", font=self.font_main_header, fg=BLACK, bg=WHITE).pack(pady=(20, 10))

        content = tk.Frame(frame, bg=WHITE)
        content.pack(fill=tk.BOTH, expand=True)

        buttons = tk.Frame(content, bg=WHITE)
        buttons.place(relx=0.5, rely=0.5, anchor="center")

        ttk.Button(buttons, text="Chi-square test (χ²)", command=lambda: self._show(self.chi2_view), style="Accent.TButton", width=CONTROL_WIDTH_CHARS).pack(pady=12, fill=tk.X)
        ttk.Button(buttons, text="T-test (Student)", command=lambda: self._show(self.ttest_view), style="Accent.TButton", width=CONTROL_WIDTH_CHARS).pack(pady=12, fill=tk.X)

        ttk.Button(frame, text="Exit", command=self.destroy, style="Action.TButton", width=CONTROL_WIDTH_CHARS).pack(side=tk.BOTTOM, padx=16, pady=16)

        return frame

    def _build_footer_grid(self, parent: tk.Frame) -> None:
        footer = tk.Frame(parent, bg=WHITE)
        footer.grid(row=2, column=0, sticky="ew", padx=16, pady=12)
        parent.grid_columnconfigure(0, weight=1)
        ttk.Separator(footer, orient=tk.HORIZONTAL).grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        ttk.Button(footer, text="Back", command=lambda: self._show(self.main_menu), style="Action.TButton", width=CONTROL_WIDTH_CHARS).grid(row=1, column=0, sticky="w")
        ttk.Button(footer, text="Exit", command=self.destroy, style="Action.TButton", width=CONTROL_WIDTH_CHARS).grid(row=1, column=1, sticky="e")

    def _build_ttest_view(self) -> tk.Frame:
        frame = tk.Frame(self.container, bg=WHITE)
        frame.grid_rowconfigure(1, weight=1)
        frame.grid_columnconfigure(0, weight=1)

        tk.Label(frame, text="T-test (Student)", font=self.font_test_header, fg=BLACK, bg=WHITE).grid(row=0, column=0, pady=(16, 8), sticky="n")

        content = tk.Frame(frame, bg=WHITE)
        content.grid(row=1, column=0, sticky="nsew", padx=16, pady=8)

        # Metric section
        tk.Label(content, text="Metric", font=self.font_ui_bold, fg=BLACK, bg=WHITE).pack(anchor="w", padx=8, pady=(8, 0))
        metric_box = tk.Frame(content, bg=WHITE)
        metric_box.pack(fill=tk.X, padx=8, pady=8)
        tk.Label(metric_box, text="Select metric:", font=self.font_ui, fg=BLACK, bg=WHITE).pack(side=tk.LEFT, padx=8, pady=8)

        metrics = [EN_METRIC_LABELS.get(k, v['name']) for k, v in t_test_queries.items()]
        self.metric_id_by_name = {EN_METRIC_LABELS.get(k, v['name']): k for k, v in t_test_queries.items()}
        self.metric_var = tk.StringVar(value=metrics[0] if metrics else "")
        create_accent_dropdown(metric_box, self.metric_var, metrics, self.font_ui, CONTROL_WIDTH_CHARS).pack(side=tk.LEFT, padx=8, pady=4)

        # Groups section
        tk.Label(content, text="Groups", font=self.font_ui_bold, fg=BLACK, bg=WHITE).pack(anchor="w", padx=8, pady=(8, 0))
        group_box = tk.Frame(content, bg=WHITE)
        group_box.pack(fill=tk.X, padx=8, pady=8)
        tk.Label(group_box, text="Compare:", font=self.font_ui, fg=BLACK, bg=WHITE).pack(side=tk.LEFT, padx=8, pady=8)

        self.group_var_t = tk.StringVar(value="Male vs Female")
        create_accent_dropdown(group_box, self.group_var_t, ["Male vs Female", "WEB vs MOBILE"], self.font_ui, CONTROL_WIDTH_CHARS).pack(side=tk.LEFT, padx=8, pady=4)

        # Actions
        actions = tk.Frame(content, bg=WHITE)
        actions.pack(fill=tk.X, padx=8, pady=8)
        ttk.Button(actions, text="Run test", command=self._on_run_ttest, style="Action.TButton", width=CONTROL_WIDTH_CHARS).pack(side=tk.LEFT, padx=4)
        ttk.Button(actions, text="Clear", command=lambda: self._set_result_text(self.result_text_ttest, ""), style="Action.TButton", width=CONTROL_WIDTH_CHARS).pack(side=tk.LEFT, padx=4)

        # Result area
        tk.Label(content, text="Result", font=self.font_ui_bold, fg=BLACK, bg=WHITE).pack(anchor="w", padx=8, pady=(8, 0))
        result_box = tk.Frame(content, bg=WHITE)
        result_box.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        self.result_text_ttest = tk.Text(result_box, height=18, wrap=tk.WORD, bg=WHITE, fg=BLACK, font=self.font_result)
        self.result_text_ttest.tag_configure("result_bold", font=self.font_result_bold)
        scrollbar_ttest = ttk.Scrollbar(result_box, orient=tk.VERTICAL, style="Accent.Vertical.TScrollbar")
        scrollbar_ttest.pack(side=tk.RIGHT, fill=tk.Y)
        self.result_text_ttest.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=6, pady=6)
        self.result_text_ttest.configure(yscrollcommand=scrollbar_ttest.set)
        scrollbar_ttest.configure(command=self.result_text_ttest.yview)

        self._build_footer_grid(frame)
        return frame

    def _build_chi2_view(self) -> tk.Frame:
        frame = tk.Frame(self.container, bg=WHITE)
        frame.grid_rowconfigure(1, weight=1)
        frame.grid_columnconfigure(0, weight=1)

        tk.Label(frame, text="Chi-square test (χ²)", font=self.font_test_header, fg=BLACK, bg=WHITE).grid(row=0, column=0, pady=(16, 8), sticky="n")

        content = tk.Frame(frame, bg=WHITE)
        content.grid(row=1, column=0, sticky="nsew", padx=16, pady=8)

        # Groups section
        tk.Label(content, text="Groups", font=self.font_ui_bold, fg=BLACK, bg=WHITE).pack(anchor="w", padx=8, pady=(8, 0))
        group_box = tk.Frame(content, bg=WHITE)
        group_box.pack(fill=tk.X, padx=8, pady=8)
        tk.Label(group_box, text="Compare distribution across:", font=self.font_ui, fg=BLACK, bg=WHITE).pack(side=tk.LEFT, padx=8, pady=8)

        self.group_var_c = tk.StringVar(value="Male vs Female")
        create_accent_dropdown(group_box, self.group_var_c, ["Male vs Female", "WEB vs MOBILE"], self.font_ui, CONTROL_WIDTH_CHARS).pack(side=tk.LEFT, padx=8, pady=4)

        # Actions
        actions = tk.Frame(content, bg=WHITE)
        actions.pack(fill=tk.X, padx=8, pady=8)
        ttk.Button(actions, text="Run test", command=self._on_run_chi2, style="Action.TButton", width=CONTROL_WIDTH_CHARS).pack(side=tk.LEFT, padx=4)
        ttk.Button(actions, text="Clear", command=lambda: self._set_result_text(self.result_text_chi2, ""), style="Action.TButton", width=CONTROL_WIDTH_CHARS).pack(side=tk.LEFT, padx=4)

        # Result area
        tk.Label(content, text="Result", font=self.font_ui_bold, fg=BLACK, bg=WHITE).pack(anchor="w", padx=8, pady=(8, 0))
        result_box = tk.Frame(content, bg=WHITE)
        result_box.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        self.result_text_chi2 = tk.Text(result_box, height=20, wrap=tk.WORD, bg=WHITE, fg=BLACK, font=self.font_result)
        self.result_text_chi2.tag_configure("result_bold", font=self.font_result_bold)
        scrollbar_chi2 = ttk.Scrollbar(result_box, orient=tk.VERTICAL, style="Accent.Vertical.TScrollbar")
        scrollbar_chi2.pack(side=tk.RIGHT, fill=tk.Y)
        self.result_text_chi2.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=6, pady=6)
        self.result_text_chi2.configure(yscrollcommand=scrollbar_chi2.set)
        scrollbar_chi2.configure(command=self.result_text_chi2.yview)

        self._build_footer_grid(frame)
        return frame

    def _show_loader(self, text: str) -> None:
        if self._loader_win:
            return
        self.update_idletasks()

        tl = tk.Toplevel(self)
        tl.transient(self)
        tl.grab_set()
        tl.configure(bg=WHITE)

        frame = tk.Frame(tl, bg=WHITE, bd=2, relief=tk.RIDGE)
        frame.pack(fill=tk.BOTH, expand=True)

        tk.Label(frame, text=text, font=self.font_ui_bold, fg=BLACK, bg=WHITE).pack(padx=20, pady=(16, 8))
        pb = ttk.Progressbar(frame, mode="indeterminate", style="Accent.Horizontal.TProgressbar")
        pb.pack(fill=tk.X, padx=20, pady=(0, 16))
        pb.start(12)

        self._center_over_parent(tl, 380, 120)
        tl.deiconify()
        tl.lift()
        self._loader_win = tl

    def _hide_loader(self) -> None:
        if self._loader_win:
            self._loader_win.grab_release()
            self._loader_win.destroy()
            self._loader_win = None

    def _center_over_parent(self, win: tk.Toplevel, width: int = 380, height: int = 120) -> None:
        self.update_idletasks()
        win.update_idletasks()
        x = self.winfo_rootx() + (self.winfo_width() // 2) - (width // 2)
        y = self.winfo_rooty() + (self.winfo_height() // 2) - (height // 2)
        win.geometry(f"{width}x{height}+{max(x, 0)}+{max(y, 0)}")

    def _set_result_text(self, text_widget: tk.Text, text: str) -> None:
        text_widget.configure(state=tk.NORMAL)
        text_widget.delete("1.0", tk.END)
        if text:
            text_widget.insert(tk.END, text)
            # Bold for "Conclusion:" line in t-test
            idx = text_widget.search("Conclusion:", "1.0", stopindex=tk.END)
            if idx:
                line_end = text_widget.index(f"{idx} lineend")
                text_widget.tag_add("result_bold", idx, line_end)
            # Bold for last line in chi-square
            last_start = text_widget.index("end-1c linestart")
            last_end = text_widget.index("end-1c")
            last_text = text_widget.get(last_start, last_end).strip()
            if last_text:
                text_widget.tag_add("result_bold", last_start, last_end)
        text_widget.configure(state=tk.DISABLED)

    def _on_run_ttest(self) -> None:
        metric_name = self.metric_var.get()
        metric_id = self.metric_id_by_name.get(metric_name)
        if not metric_id:
            messagebox.showerror("Error", "Select a valid metric")
            return
        group_choice = 1 if self.group_var_t.get() == "Male vs Female" else 2

        def worker():
            ok, msg = run_ttest_gui(metric_id, group_choice)
            self.after(0, lambda: self._finish_test(ok, msg, self.result_text_ttest, "t-test"))

        self._show_loader("Running t-test…")
        threading.Thread(target=worker, daemon=True).start()

    def _on_run_chi2(self) -> None:
        group_choice = 1 if self.group_var_c.get() == "Male vs Female" else 2

        def worker():
            ok, msg = run_chi_square_gui(group_choice)
            self.after(0, lambda: self._finish_test(ok, msg, self.result_text_chi2, "Chi-square test"))

        self._show_loader("Running chi-square test…")
        threading.Thread(target=worker, daemon=True).start()

    def _finish_test(self, ok: bool, msg: str, text_widget: tk.Text, test_name: str) -> None:
        self._hide_loader()
        if not ok:
            messagebox.showerror(test_name, msg)
        self._set_result_text(text_widget, msg)

if __name__ == "__main__":
    app = StatTestsApp()
    app.mainloop()
