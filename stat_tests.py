import os
import threading
import threading
import tkinter as tk
from tkinter import ttk, messagebox
from tkinter import font as tkfont
from typing import Optional, Tuple

import pandas as pd
from scipy import stats

# Colors
WHITE = "#ffffff"
BLACK = "#000000"
ACCENT = "#ecf573"  # buttons and dropdowns only

# Try to reuse existing t-test module (DB engine, queries, helpers)
ENGINE = None
TEST_QUERIES = None
IMPORT_ERROR: Optional[Exception] = None
try:
    import stattest as tmod  # existing CLI t-test script
    ENGINE = getattr(tmod, "engine", None)
    TEST_QUERIES = getattr(tmod, "TEST_QUERIES", None)
except Exception as e:  # pragma: no cover
    IMPORT_ERROR = e

# Optional local font loader (tkextrafont)
HAS_EXTRA_FONT = False
try:
    from tkextrafont import Font as ExtraFont  # pip install tkextrafont

    def load_local_fonts() -> None:
        global HAS_EXTRA_FONT
        base_dir = os.path.dirname(os.path.abspath(__file__))
        fonts_dir = os.path.join(base_dir, "fonts")
        try:
            reg_path = os.path.join(fonts_dir, "Montserrat-Regular.ttf")
            if os.path.exists(reg_path):
                ExtraFont(file=reg_path, family="Montserrat")
            med_it_path = os.path.join(fonts_dir, "Montserrat-MediumItalic.ttf")
            if os.path.exists(med_it_path):
                ExtraFont(file=med_it_path, family="Montserrat Medium Italic")
            bold_path = os.path.join(fonts_dir, "Montserrat-Bold.ttf")
            if os.path.exists(bold_path):
                ExtraFont(file=bold_path, family="Montserrat Bold")
            HAS_EXTRA_FONT = True
        except Exception:
            HAS_EXTRA_FONT = False

except Exception:
    def load_local_fonts() -> None:
        pass

# Limit t-test metrics and provide English labels
ALLOWED_TTEST_IDS = [1, 2, 3, 4, 5]
EN_METRIC_LABELS = {
    1: "Average order value",
    2: "Number of items per order",
    3: "Number of clicks before booking",
    4: "Average delivery cost",
    5: "Number of unique products",
}

# Unified control width (characters) for dropdowns and action buttons on test screens
CONTROL_WIDTH_CHARS = 28


def run_ttest_gui(metric_id: int, group_choice: int) -> Tuple[bool, str]:
    """Run a t-test using aggregated stats from SQL via existing engine/queries.

    Returns (ok, message_text)
    """
    if ENGINE is None or TEST_QUERIES is None:
        return False, (
            "Error: Failed to initialize DB connection or tests.\n"
            "Check 'stattest.py' and 'config.yaml'."
        )

    try:
        test_info = TEST_QUERIES[metric_id]
    except Exception:
        return False, f"Error: Unknown metric id={metric_id}."

    metric_label_en = EN_METRIC_LABELS.get(metric_id, test_info.get("name", str(metric_id)))

    sql_query = (
        test_info["sql_male_female"] if group_choice == 1 else test_info["sql_web_mobile"]
    )
    group1_name, group2_name = ("M", "F") if group_choice == 1 else ("WEB", "MOBILE")

    try:
        df_stats = pd.read_sql(sql_query, ENGINE)
    except Exception as e:
        return False, f"SQL error: {e}"

    if "group_name" not in df_stats.columns:
        return False, "Error: Result set is missing column 'group_name'."

    try:
        if len(df_stats) != 2:
            return False, (
                "Error: t-test requires exactly 2 groups.\n"
                f"Rows: {len(df_stats)}; groups: {df_stats['group_name'].astype(str).tolist()}"
            )

        # Normalize group names and match exactly (case-insensitive)
        df_stats = df_stats.copy()
        df_stats["group_norm"] = df_stats["group_name"].astype(str).str.strip().str.upper()

        group1_stats = df_stats[df_stats["group_norm"] == group1_name]
        group2_stats = df_stats[df_stats["group_norm"] == group2_name]
        if group1_stats.empty or group2_stats.empty:
            return False, (
                f"Error: Missing data for {group1_name} or {group2_name}.\n"
                f"Available groups: {df_stats['group_name'].astype(str).tolist()}"
            )

        mean1, std1, n1 = group1_stats.iloc[0][["mean", "stddev", "count"]]
        mean2, std2, n2 = group2_stats.iloc[0][["mean", "stddev", "count"]]

        # Prefer using the helper if available
        try:
            t_stat, p_val = tmod.perform_ttest_from_stats(
                float(mean1), float(std1), int(n1), float(mean2), float(std2), int(n2), metric_label_en, group1_name, group2_name
            )
        except Exception:
            res = stats.ttest_ind_from_stats(
                mean1=float(mean1), std1=float(std1), nobs1=int(n1),
                mean2=float(mean2), std2=float(std2), nobs2=int(n2),
                equal_var=False
            )
            t_stat, p_val = float(res.statistic), float(res.pvalue)

        if p_val is None:
            return False, "Warning: Insufficient data to compute t-test."

        decision = (
            "Significant difference" if p_val < 0.05 else "No statistically significant difference"
        )
        result_text = (
            f"t-test — {metric_label_en}\n"
            f"Groups: {group1_name} vs {group2_name}\n"
            f"t-statistic: {t_stat:.4f}\n"
            f"p-value: {p_val:.4f}\n"
            f"Conclusion: {decision} (alpha=0.05)"
        )
        return True, result_text
    except Exception as e:  # pragma: no cover
        return False, f"Error while running t-test: {e}"


def run_chi_square_gui(group_choice: int) -> Tuple[bool, str]:
    """Run a Chi-square test mirroring the reference notebook output.

    - Builds a row-per-customer dataset, then crosstab by payment_method x Group
    - Prints counts table, column-normalized percentages, then H0 statement and decision
    """
    if ENGINE is None:
        return False, (
            "Error: Failed to initialize DB connection.\n"
            "Check 'stattest.py' and 'config.yaml'."
        )

    if group_choice == 1:
        group_col = "gender"
        group_title = "Gender"
        sql_candidates = [
            # Prefer the dm_transactions variant (as in the notebook)
            (
                "SELECT t.payment_method AS payment_method, "
                "c.gender AS gender, "
                "c.customer_id AS customer_id, "
                "COUNT(*) AS transactions_cnt "
                "FROM dm_transactions t INNER JOIN rd_customers c ON t.customer_id = c.customer_id "
                "WHERE c.gender IN ('M','F') "
                "GROUP BY t.payment_method, c.gender, c.customer_id"
            ),
            # Fallback to rd_transactions
            (
                "SELECT t.payment_method AS payment_method, "
                "c.gender AS gender, "
                "c.customer_id AS customer_id, "
                "COUNT(*) AS transactions_cnt "
                "FROM rd_transactions t INNER JOIN rd_customers c ON t.customer_id = c.customer_id "
                "WHERE c.gender IN ('M','F') "
                "GROUP BY t.payment_method, c.gender, c.customer_id"
            ),
        ]
    else:
        group_col = "traffic_source"
        group_title = "Traffic source"
        sql_candidates = [
            (
                "SELECT t.payment_method AS payment_method, "
                "s.traffic_source AS traffic_source, "
                "c.customer_id AS customer_id, "
                "COUNT(*) AS transactions_cnt "
                "FROM rd_transactions t "
                "INNER JOIN rd_sessions s ON t.session_id = s.session_id "
                "INNER JOIN rd_customers c ON t.customer_id = c.customer_id "
                "WHERE s.traffic_source IN ('WEB','MOBILE') "
                "GROUP BY t.payment_method, s.traffic_source, c.customer_id"
            ),
        ]

    last_error: Optional[Exception] = None
    df: Optional[pd.DataFrame] = None
    for sql in sql_candidates:
        try:
            df = pd.read_sql(sql, ENGINE)
            break
        except Exception as e:
            last_error = e
            df = None
            continue

    if df is None or df.empty:
        if last_error is not None:
            return False, f"SQL error: {last_error}"
        return False, "Error: Empty SQL result for Chi-square test."

    try:
        contingency_table = pd.crosstab(df["payment_method"], df[group_col])
        contingency_table_percent = (
            pd.crosstab(df["payment_method"], df[group_col], normalize="columns") * 100.0
        ).round(0)

        chi2_stat, p_value, dof, expected = stats.chi2_contingency(contingency_table)

        # Build output exactly like the reference
        lines: list[str] = []
        lines.append(f"Does percentage of Payment_method differ across {group_title}? \n")
        lines.append("Number of customers:\n")
        lines.append(contingency_table.to_string())
        lines.append("\n Number of customers. % of totals by " + group_title + ":  \n")
        lines.append(contingency_table_percent.to_string() + " \n")
        lines.append(
            f"Null hypothesis: Payment_method distribution is independent of {group_title}."
        )
        lines.append("P-value of Chi-square test =  " + str(round(float(p_value), 3)))
        if float(p_value) < 0.05:
            lines.append("We reject Null hypothesis. \n")
            lines.append(
                "There is statistical evidence that Payment_method distribution differs across "
                + group_title + "."
            )
        else:
            lines.append("We fail to reject the null hypothesis \n")
            lines.append(
                "There is NO statistically significant evidence that Payment_method distribution differs across "
                + group_title + "."
            )

        result_text = "\n".join(lines)
        return True, result_text
    except Exception as e:
        return False, f"Error while running Chi-square test: {e}"


def create_accent_dropdown(parent: tk.Widget, variable: tk.StringVar, values: list[str], font: tkfont.Font, width_chars: int) -> tk.OptionMenu:
    # Styled tk.OptionMenu (menubutton) with accent background and custom font
    if not values:
        values = [""]
    if variable.get() not in values:
        variable.set(values[0])
    om = tk.OptionMenu(parent, variable, *values)
    om.config(
        font=font,
        bg=ACCENT,
        fg=BLACK,
        activebackground=ACCENT,
        activeforeground=BLACK,
        highlightthickness=1,
        bd=1,
        width=width_chars,
    )
    menu = om.nametowidget(om.menuname)
    menu.config(bg=ACCENT, fg=BLACK, activebackground=ACCENT, activeforeground=BLACK, font=font)
    return om


class StatTestsApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Statistical Tests")
        self.geometry("980x720")
        self.minsize(900, 620)

        # Load local fonts if available
        load_local_fonts()

        # Root white background
        self.configure(bg=WHITE)

        # Top-level container
        self.container = tk.Frame(self, bg=WHITE)
        self.container.pack(fill=tk.BOTH, expand=True)

        self._init_fonts()
        self._init_styles()

        # Loader modal
        self._loader_win: Optional[tk.Toplevel] = None

        # Views
        self.main_menu = self._build_main_menu(self.container)
        self.ttest_view = self._build_ttest_view(self.container)
        self.chi2_view = self._build_chi2_view(self.container)

        # Start in main menu
        self._show(self.main_menu)

        # DB warn
        if ENGINE is None or TEST_QUERIES is None:
            detail = str(IMPORT_ERROR) if IMPORT_ERROR else "Unknown error."
            messagebox.showwarning(
                "Database initialization",
                "Failed to initialize DB connection or load tests.\n"
                "Check 'config.yaml' and module 'stattest.py'.\n\n"
                f"Details: {detail}"
            )

    def _init_fonts(self) -> None:
        # Base UI font increased by 2pt; result text is base-2
        base = 16
        main_title = int(base * 2.25)  # 2–2.4x
        test_title = int(base * 1.3)   # 1.2–1.5x

        # Use local font families if loaded
        family_regular = "Montserrat"
        # Dropdowns match button font/size (no italic per latest request)
        self.font_ui = tkfont.Font(family=family_regular, size=base)
        self.font_ui_bold = tkfont.Font(family=family_regular, size=base, weight="bold")
        self.font_dropdown = self.font_ui
        self.font_main_header = tkfont.Font(family=family_regular, size=main_title, weight="bold")
        self.font_test_header = tkfont.Font(family=family_regular, size=test_title, weight="bold")
        self.font_result = tkfont.Font(family=family_regular, size=base - 2)
        self.font_result_bold = tkfont.Font(family=family_regular, size=base - 2, weight="bold")

    def _init_styles(self) -> None:
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        # Buttons (accent) — action buttons ~25% less tall (vertical padding)
        style.configure(
            "Accent.TButton",
            font=self.font_ui,
            padding=(20, 12),
            background=ACCENT,
            foreground=BLACK,
            borderwidth=1,
        )
        style.map(
            "Accent.TButton",
            background=[("active", ACCENT)],
            foreground=[("active", BLACK)],
        )

        style.configure(
            "Action.TButton",
            font=self.font_ui,
            padding=(20, 8),
            background=ACCENT,
            foreground=BLACK,
            borderwidth=1,
        )
        style.map(
            "Action.TButton",
            background=[("active", ACCENT)],
            foreground=[("active", BLACK)],
        )

        # Accent scrollbar style (platform-dependent)
        style.configure("Accent.Vertical.TScrollbar", background=ACCENT, troughcolor=WHITE)

        # Accent progressbar for loader
        style.configure("Accent.Horizontal.TProgressbar", troughcolor=WHITE, background=ACCENT)

    def _show(self, frame: tk.Frame) -> None:
        for child in self.container.winfo_children():
            child.pack_forget()
        frame.pack(fill=tk.BOTH, expand=True)

    def _build_main_menu(self, parent: tk.Frame) -> tk.Frame:
        frame = tk.Frame(parent, bg=WHITE)

        # Title centered top, black
        title = tk.Label(frame, text="Statistical Tests", font=self.font_main_header, fg=BLACK, bg=WHITE)
        title.pack(pady=(20, 10))

        content = tk.Frame(frame, bg=WHITE)
        content.pack(fill=tk.BOTH, expand=True)

        # Vertical centered buttons
        buttons = tk.Frame(content, bg=WHITE)
        buttons.place(relx=0.5, rely=0.5, anchor="center")

        chi2_btn = ttk.Button(
            buttons,
            text="Chi-square test (χ²)",
            command=lambda: self._show(self.chi2_view),
            style="Accent.TButton",
            width=CONTROL_WIDTH_CHARS,
        )
        chi2_btn.pack(pady=12, fill=tk.X)

        ttest_btn = ttk.Button(
            buttons,
            text="T-test (Student)",
            command=lambda: self._show(self.ttest_view),
            style="Accent.TButton",
            width=CONTROL_WIDTH_CHARS,
        )
        ttest_btn.pack(pady=12, fill=tk.X)

        quit_btn = ttk.Button(frame, text="Exit", command=self.destroy, style="Action.TButton", width=CONTROL_WIDTH_CHARS)
        quit_btn.pack(side=tk.BOTTOM, padx=16, pady=16)

        return frame

    def _build_footer_grid(self, parent: tk.Frame, row: int) -> None:
        footer = tk.Frame(parent, bg=WHITE)
        footer.grid(row=row, column=0, sticky="ew", padx=16, pady=12)
        footer.grid_columnconfigure(0, weight=1)
        sep = ttk.Separator(footer, orient=tk.HORIZONTAL)
        sep.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        back_btn = ttk.Button(footer, text="Back", command=lambda: self._show(self.main_menu), style="Action.TButton", width=CONTROL_WIDTH_CHARS)
        back_btn.grid(row=1, column=0, sticky="w")
        exit_btn = ttk.Button(footer, text="Exit", command=self.destroy, style="Action.TButton", width=CONTROL_WIDTH_CHARS)
        exit_btn.grid(row=1, column=1, sticky="e")

    def _build_ttest_view(self, parent: tk.Frame) -> tk.Frame:
        frame = tk.Frame(parent, bg=WHITE)
        frame.grid_rowconfigure(1, weight=1)
        frame.grid_columnconfigure(0, weight=1)

        # Header centered top, black
        header = tk.Label(frame, text="T-test (Student)", font=self.font_test_header, fg=BLACK, bg=WHITE)
        header.grid(row=0, column=0, pady=(16, 8), sticky="n")

        content = tk.Frame(frame, bg=WHITE)
        content.grid(row=1, column=0, sticky="nsew", padx=16, pady=8)

        # Metric section (bold heading)
        metric_label = tk.Label(content, text="Metric", font=self.font_ui_bold, fg=BLACK, bg=WHITE)
        metric_label.pack(anchor="w", padx=8, pady=(8, 0))
        metric_box = tk.Frame(content, bg=WHITE)
        metric_box.pack(fill=tk.X, padx=8, pady=8)

        metrics: list[str] = []
        self.metric_id_by_name: dict[str, int] = {}
        if TEST_QUERIES is not None:
            for metric_id in ALLOWED_TTEST_IDS:
                if metric_id in TEST_QUERIES:
                    label = EN_METRIC_LABELS.get(metric_id, str(metric_id))
                    metrics.append(label)
                    self.metric_id_by_name[label] = metric_id
        else:
            for metric_id in ALLOWED_TTEST_IDS:
                label = EN_METRIC_LABELS.get(metric_id, str(metric_id))
                metrics.append(label)
                self.metric_id_by_name[label] = metric_id

        tk.Label(metric_box, text="Select metric:", font=self.font_ui, fg=BLACK, bg=WHITE).pack(
            side=tk.LEFT, padx=8, pady=8
        )
        self.metric_var = tk.StringVar(value=metrics[0] if metrics else "")
        metric_dd = create_accent_dropdown(metric_box, self.metric_var, metrics, self.font_ui, width_chars=CONTROL_WIDTH_CHARS)
        metric_dd.pack(side=tk.LEFT, padx=8, pady=4)

        # Group section (bold heading)
        group_label = tk.Label(content, text="Groups", font=self.font_ui_bold, fg=BLACK, bg=WHITE)
        group_label.pack(anchor="w", padx=8, pady=(8, 0))
        group_box = tk.Frame(content, bg=WHITE)
        group_box.pack(fill=tk.X, padx=8, pady=8)

        tk.Label(group_box, text="Compare:", font=self.font_ui, fg=BLACK, bg=WHITE).pack(
            side=tk.LEFT, padx=8, pady=8
        )
        self.group_var_t = tk.StringVar(value="Male vs Female")
        groups_list = ["Male vs Female", "WEB vs MOBILE"]
        groups_dd = create_accent_dropdown(group_box, self.group_var_t, groups_list, self.font_ui, width_chars=CONTROL_WIDTH_CHARS)
        groups_dd.pack(side=tk.LEFT, padx=8, pady=4)

        # Actions
        actions = tk.Frame(content, bg=WHITE)
        actions.pack(fill=tk.X, padx=8, pady=8)
        run_btn = ttk.Button(actions, text="Run test", command=self._on_run_ttest, style="Action.TButton", width=CONTROL_WIDTH_CHARS)
        run_btn.pack(side=tk.LEFT, padx=4)
        clear_btn = ttk.Button(
            actions, text="Clear", command=lambda: self._set_result_text(self.result_text_ttest, ""), style="Action.TButton", width=CONTROL_WIDTH_CHARS
        )
        clear_btn.pack(side=tk.LEFT, padx=4)

        # Result area
        result_label = tk.Label(content, text="Result", font=self.font_ui_bold, fg=BLACK, bg=WHITE)
        result_label.pack(anchor="w", padx=8, pady=(8, 0))
        result_box = tk.Frame(content, bg=WHITE)
        result_box.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        self.result_text_ttest = tk.Text(result_box, height=18, wrap=tk.WORD, bg=WHITE, fg=BLACK)
        self.result_text_ttest.configure(font=self.font_result)
        self.result_text_ttest.tag_configure("result_bold", font=self.font_result_bold)
        # Scrollbar
        sb_t = ttk.Scrollbar(result_box, orient=tk.VERTICAL, command=self.result_text_ttest.yview, style="Accent.Vertical.TScrollbar")
        sb_t.pack(side=tk.RIGHT, fill=tk.Y)
        self.result_text_ttest.configure(yscrollcommand=sb_t.set)
        self.result_text_ttest.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=6, pady=6)

        # Footer (Back/Exit) - grid-fixed bottom
        self._build_footer_grid(frame, row=2)
        return frame

    def _build_chi2_view(self, parent: tk.Frame) -> tk.Frame:
        frame = tk.Frame(parent, bg=WHITE)
        frame.grid_rowconfigure(1, weight=1)
        frame.grid_columnconfigure(0, weight=1)

        header = tk.Label(frame, text="Chi-square test (χ²)", font=self.font_test_header, fg=BLACK, bg=WHITE)
        header.grid(row=0, column=0, pady=(16, 8), sticky="n")

        content = tk.Frame(frame, bg=WHITE)
        content.grid(row=1, column=0, sticky="nsew", padx=16, pady=8)

        # Group section (bold heading)
        group_label = tk.Label(content, text="Groups", font=self.font_ui_bold, fg=BLACK, bg=WHITE)
        group_label.pack(anchor="w", padx=8, pady=(8, 0))
        group_box = tk.Frame(content, bg=WHITE)
        group_box.pack(fill=tk.X, padx=8, pady=8)

        tk.Label(group_box, text="Compare distribution across:", font=self.font_ui, fg=BLACK, bg=WHITE).pack(
            side=tk.LEFT, padx=8, pady=8
        )
        self.group_var_c = tk.StringVar(value="Male vs Female")
        groups_list = ["Male vs Female", "WEB vs MOBILE"]
        groups_dd = create_accent_dropdown(group_box, self.group_var_c, groups_list, self.font_ui, width_chars=CONTROL_WIDTH_CHARS)
        groups_dd.pack(side=tk.LEFT, padx=8, pady=4)

        # Actions
        actions = tk.Frame(content, bg=WHITE)
        actions.pack(fill=tk.X, padx=8, pady=8)
        run_btn = ttk.Button(actions, text="Run test", command=self._on_run_chi2, style="Action.TButton", width=CONTROL_WIDTH_CHARS)
        run_btn.pack(side=tk.LEFT, padx=4)
        clear_btn = ttk.Button(
            actions, text="Clear", command=lambda: self._set_result_text(self.result_text_chi2, ""), style="Action.TButton", width=CONTROL_WIDTH_CHARS
        )
        clear_btn.pack(side=tk.LEFT, padx=4)

        # Result area
        result_label = tk.Label(content, text="Result", font=self.font_ui_bold, fg=BLACK, bg=WHITE)
        result_label.pack(anchor="w", padx=8, pady=(8, 0))
        result_box = tk.Frame(content, bg=WHITE)
        result_box.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        self.result_text_chi2 = tk.Text(result_box, height=20, wrap=tk.WORD, bg=WHITE, fg=BLACK)
        self.result_text_chi2.configure(font=self.font_result)
        self.result_text_chi2.tag_configure("result_bold", font=self.font_result_bold)
        # Scrollbar
        sb_c = ttk.Scrollbar(result_box, orient=tk.VERTICAL, command=self.result_text_chi2.yview, style="Accent.Vertical.TScrollbar")
        sb_c.pack(side=tk.RIGHT, fill=tk.Y)
        self.result_text_chi2.configure(yscrollcommand=sb_c.set)
        self.result_text_chi2.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=6, pady=6)

        # Footer (Back/Exit) - grid-fixed bottom
        self._build_footer_grid(frame, row=2)
        return frame

    def _center_over_parent(self, win: tk.Toplevel, width: int = 360, height: int = 120) -> None:
        self.update_idletasks()
        win.update_idletasks()
        x = self.winfo_rootx() + (self.winfo_width() // 2) - (width // 2)
        y = self.winfo_rooty() + (self.winfo_height() // 2) - (height // 2)
        win.geometry(f"{width}x{height}+{max(x, 0)}+{max(y, 0)}")

    def _show_loader(self, text: str) -> None:
        if self._loader_win is not None:
            return
        # Ensure pending draws happen before creating modal
        self.update_idletasks()

        tl = tk.Toplevel(self)
        tl.withdraw()  # map only after configured
        tl.transient(self)
        tl.grab_set()
        # Use normal toplevel (no overrideredirect) for better WM compatibility
        tl.configure(bg=WHITE)

        frame = tk.Frame(tl, bg=WHITE, bd=2, relief=tk.RIDGE)
        frame.pack(fill=tk.BOTH, expand=True)

        lbl = tk.Label(frame, text=text, font=self.font_ui_bold, fg=BLACK, bg=WHITE)
        lbl.pack(padx=20, pady=(16, 8))

        pb = ttk.Progressbar(frame, mode="indeterminate", style="Accent.Horizontal.TProgressbar")
        pb.pack(fill=tk.X, padx=20, pady=(0, 16))
        pb.start(12)

        self._center_over_parent(tl, 380, 120)
        tl.deiconify()
        tl.lift()
        try:
            tl.attributes('-topmost', True)
        except Exception:
            pass
        tl.update_idletasks()
        tl.update()  # force a draw before long-running work
        try:
            tl.attributes('-topmost', False)
        except Exception:
            pass
        self._loader_win = tl

    def _hide_loader(self) -> None:
        if self._loader_win is not None:
            try:
                self._loader_win.grab_release()
            except Exception:
                pass
            self._loader_win.destroy()
            self._loader_win = None

    def _set_result_text(self, text_widget: tk.Text, text: str) -> None:
        text_widget.configure(state=tk.NORMAL)
        text_widget.delete("1.0", tk.END)
        if text:
            text_widget.insert(tk.END, text)
            # Bold the line(s) starting with 'Conclusion:' (t-test)
            idx = "1.0"
            while True:
                idx = text_widget.search("Conclusion:", idx, stopindex=tk.END)
                if not idx:
                    break
                line_end = text_widget.index(f"{idx} lineend")
                text_widget.tag_add("result_bold", idx, line_end)
                idx = line_end
            # Also bold the very last non-empty line (chi-square)
            last_end = text_widget.index("end-1c")
            last_start = text_widget.index("end-1c linestart")
            last_text = text_widget.get(last_start, last_end).strip()
            if last_text:
                text_widget.tag_add("result_bold", last_start, last_end)
        text_widget.configure(state=tk.NORMAL)

    # Event handlers
    def _on_run_ttest(self) -> None:
        metric_name = self.metric_var.get()
        if not metric_name:
            messagebox.showerror("Error", "Select a metric")
            return
        metric_id = self.metric_id_by_name.get(metric_name)
        if metric_id is None:
            messagebox.showerror("Error", "Invalid metric selected")
            return
        group_choice = 1 if self.group_var_t.get().startswith("Male") else 2

        def worker() -> None:
            try:
                ok, msg = run_ttest_gui(metric_id, group_choice)
            except Exception as e:
                ok, msg = False, f"Unexpected error: {e}"
            def done() -> None:
                self._hide_loader()
                if not ok:
                    messagebox.showerror("t-test", msg)
                self._set_result_text(self.result_text_ttest, msg)
            self.after(0, done)

        self._show_loader("Running t-test…")
        threading.Thread(target=worker, daemon=True).start()

    def _on_run_chi2(self) -> None:
        group_choice = 1 if self.group_var_c.get().startswith("Male") else 2

        def worker() -> None:
            try:
                ok, msg = run_chi_square_gui(group_choice)
            except Exception as e:
                ok, msg = False, f"Unexpected error: {e}"
            def done() -> None:
                self._hide_loader()
                if not ok:
                    messagebox.showerror("Chi-square test", msg)
                self._set_result_text(self.result_text_chi2, msg)
            self.after(0, done)

        self._show_loader("Running chi-square…")
        threading.Thread(target=worker, daemon=True).start()


if __name__ == "__main__":
    app = StatTestsApp()
    app.mainloop()