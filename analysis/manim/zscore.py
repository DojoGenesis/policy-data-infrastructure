"""
Z-Score Normalization Animation
Policy Data Infrastructure — ADR-007 stats animation series

Narrative: Why raw policy data (poverty %, income $) can't be compared directly,
and how z-score normalization puts everything on the same scale.

Style: Night Shift palette — dark background (#0a0a0f), amber accent, system font.
No voiceover — text labels carry the explanation.
Target: 60-90 seconds, single scene with four phases.
"""

from manim import *

# ── Palette ──────────────────────────────────────────────────────────────────
BG = "#0a0a0f"
AMBER = "#f59e0b"       # accent — transformed values, highlights
GRAY = "#6b7280"         # raw values, muted elements
SUBTLE = "#3f3f46"       # axes, grid lines, structural
WHITE = "#e4e4e7"        # primary text
DIM = "#52525b"           # secondary text
CURVE = "#8b5cf6"          # bell curve color (violet, complementary to amber)
CURVE_FILL = "#8b5cf6"     # bell curve fill
MONO = "Menlo"

# ── Data ─────────────────────────────────────────────────────────────────────
# Poverty rates (percentages)
POVERTY_VALUES = [5, 12, 22, 35, 45]
POVERTY_LABELS = ["5%", "12%", "22%", "35%", "45%"]
POVERTY_MEAN = 23.8
POVERTY_STD = 15.0

# Median incomes ($K)
INCOME_VALUES = [32, 48, 65, 88, 115]
INCOME_LABELS = ["$32K", "$48K", "$65K", "$88K", "$115K"]
INCOME_MEAN = 69.6
INCOME_STD = 30.0

# ── Helpers ──────────────────────────────────────────────────────────────────
def z_score(x, mu, sigma):
    return (x - mu) / sigma

def make_text(s, font_size=24, color=WHITE, weight=None):
    """Create a Text mobject with the project's monospace font."""
    kwargs = {"font_size": font_size, "color": color, "font": MONO}
    if weight:
        kwargs["weight"] = weight
    return Text(s, **kwargs)


class ZScore(Scene):
    """Single-scene z-score explainer: raw → transform → bell curve → insight."""

    def construct(self):
        self.camera.background_color = BG

        # ═══════════════════════════════════════════════════════════════════
        # PHASE 1: The Problem — Raw Values (0s – 18s)
        # ═══════════════════════════════════════════════════════════════════
        self.phase1_raw_values()

        # ═══════════════════════════════════════════════════════════════════
        # PHASE 2: The Transformation — Z-Score (18s – 48s)
        # ═══════════════════════════════════════════════════════════════════
        self.phase2_transformation()

        # ═══════════════════════════════════════════════════════════════════
        # PHASE 3: The Bell Curve (48s – 72s)
        # ═══════════════════════════════════════════════════════════════════
        self.phase3_bell_curve()

        # ═══════════════════════════════════════════════════════════════════
        # PHASE 4: The Insight (72s – 88s)
        # ═══════════════════════════════════════════════════════════════════
        self.phase4_insight()

    def phase1_raw_values(self):
        """Show raw poverty rates and median incomes on separate number lines
        — different units, not comparable."""

        # ── Title ──────────────────────────────────────────────────────────
        title = make_text("THE PROBLEM", font_size=36, color=WHITE, weight=BOLD)
        title.to_edge(UP, buff=0.6)
        self.play(Write(title), run_time=1.5)
        self.wait(0.5)

        subtitle = make_text(
            "Different units. Different scales. Not comparable.",
            font_size=20, color=DIM
        )
        subtitle.next_to(title, DOWN, buff=0.25)
        self.play(FadeIn(subtitle), run_time=1.0)
        self.wait(1.0)

        # ── Poverty Number Line (top) ──────────────────────────────────────
        pov_label = make_text("POVERTY RATE", font_size=18, color=GRAY)
        pov_label.shift(UP * 2.2 + RIGHT * 0.5)

        pov_axes = Axes(
            x_range=[0, 50, 10],
            y_range=[0, 1, 1],
            x_length=10,
            y_length=0.8,
            axis_config={
                "include_numbers": True,
                "font_size": 16,
                "color": SUBTLE,
                "stroke_width": 1.5,
            },
            x_axis_config={
                "numbers_to_include": [0, 10, 20, 30, 40, 50],
                "label_direction": DOWN,
            },
            y_axis_config={"stroke_width": 0},
        )
        pov_axes.shift(UP * 1.0)
        pov_axes.set_opacity(0.5)

        pov_unit = make_text("(%)", font_size=14, color=GRAY)
        pov_unit.next_to(pov_axes.get_right(), RIGHT, buff=0.2).shift(UP * 0.05)

        # ── Income Number Line (bottom) ────────────────────────────────────
        inc_label = make_text("MEDIAN INCOME", font_size=18, color=GRAY)
        inc_label.shift(DOWN * 0.3 + RIGHT * 0.5)

        inc_axes = Axes(
            x_range=[20, 130, 20],
            y_range=[0, 1, 1],
            x_length=10,
            y_length=0.8,
            axis_config={
                "include_numbers": True,
                "font_size": 16,
                "color": SUBTLE,
                "stroke_width": 1.5,
            },
            x_axis_config={
                "numbers_to_include": [20, 40, 60, 80, 100, 120],
                "label_direction": DOWN,
            },
            y_axis_config={"stroke_width": 0},
        )
        inc_axes.shift(DOWN * 1.8)
        inc_axes.set_opacity(0.5)

        inc_unit = make_text("($K)", font_size=14, color=GRAY)
        inc_unit.next_to(inc_axes.get_right(), RIGHT, buff=0.2).shift(UP * 0.05)

        # ── Animate axes and labels ────────────────────────────────────────
        self.play(
            LaggedStart(
                Create(pov_axes, run_time=1.0),
                FadeIn(pov_label, run_time=0.8),
                FadeIn(pov_unit, run_time=0.5),
                lag_ratio=0.3,
            ),
            LaggedStart(
                Create(inc_axes, run_time=1.0),
                FadeIn(inc_label, run_time=0.8),
                FadeIn(inc_unit, run_time=0.5),
                lag_ratio=0.3,
            ),
            run_time=2.5,
        )
        self.wait(0.5)

        # ── Plot poverty dots ──────────────────────────────────────────────
        pov_dots = VGroup()
        pov_value_labels = VGroup()
        for i, (val, lbl) in enumerate(zip(POVERTY_VALUES, POVERTY_LABELS)):
            dot = Dot(
                pov_axes.c2p(val, 0.5),
                color=GRAY,
                radius=0.08,
            )
            label = make_text(lbl, font_size=16, color=GRAY)
            label.next_to(dot, UP, buff=0.15)
            pov_dots.add(dot)
            pov_value_labels.add(label)

        # ── Plot income dots ───────────────────────────────────────────────
        inc_dots = VGroup()
        inc_value_labels = VGroup()
        for i, (val, lbl) in enumerate(zip(INCOME_VALUES, INCOME_LABELS)):
            dot = Dot(
                inc_axes.c2p(val, 0.5),
                color=GRAY,
                radius=0.08,
            )
            label = make_text(lbl, font_size=16, color=GRAY)
            label.next_to(dot, UP, buff=0.15)
            inc_dots.add(dot)
            inc_value_labels.add(label)

        self.play(
            LaggedStart(
                *[GrowFromCenter(d) for d in pov_dots],
                *[FadeIn(l) for l in pov_value_labels],
                lag_ratio=0.15,
                run_time=2.0,
            ),
            LaggedStart(
                *[GrowFromCenter(d) for d in inc_dots],
                *[FadeIn(l) for l in inc_value_labels],
                lag_ratio=0.15,
                run_time=2.0,
            ),
        )
        self.wait(1.0)

        # ── "Not comparable" callout ───────────────────────────────────────
        not_comp = make_text(
            "Can't compare % to $ — different scales, different meanings",
            font_size=18, color=DIM
        )
        not_comp.shift(DOWN * 3.1)
        self.play(FadeIn(not_comp), run_time=1.0)
        self.wait(1.5)

        # Store for later use
        self.pov_axes = pov_axes
        self.inc_axes = inc_axes
        self.pov_dots = pov_dots
        self.pov_value_labels = pov_value_labels
        self.inc_dots = inc_dots
        self.inc_value_labels = inc_value_labels
        self.pov_label = pov_label
        self.inc_label = inc_label
        self.pov_unit = pov_unit
        self.inc_unit = inc_unit

        # ── Clean exit: fade out title elements, keep axes ─────────────────
        self.play(
            FadeOut(title),
            FadeOut(subtitle),
            FadeOut(not_comp),
            run_time=0.8,
        )
        self.wait(0.3)

    def phase2_transformation(self):
        """Show the z-score formula and animate values sliding to standardized
        positions on a common number line."""

        # ── Phase title ────────────────────────────────────────────────────
        phase_title = make_text("Z-SCORE TRANSFORMATION", font_size=30, color=AMBER, weight=BOLD)
        phase_title.to_edge(UP, buff=0.6)
        self.play(Write(phase_title), run_time=1.5)
        self.wait(0.5)

        # ── Show formula ───────────────────────────────────────────────────
        formula = MathTex(
            r"z = \frac{x - \mu}{\sigma}",
            font_size=42,
            color=WHITE,
        )
        formula.shift(UP * 0.3)
        self.play(Write(formula), run_time=2.0)
        self.wait(1.0)

        formula_label = make_text(
            "x = raw value    μ = mean    σ = std dev",
            font_size=16, color=DIM
        )
        formula_label.next_to(formula, DOWN, buff=0.3)
        self.play(FadeIn(formula_label), run_time=1.0)
        self.wait(1.0)

        # ── Show μ and σ for each dataset ──────────────────────────────────
        pov_stats = make_text(
            f"Poverty: μ={POVERTY_MEAN}%  σ={POVERTY_STD:.1f}",
            font_size=16, color=GRAY
        )
        inc_stats = make_text(
            f"Income:  μ=${INCOME_MEAN:.0f}K  σ=${INCOME_STD:.0f}K",
            font_size=16, color=GRAY
        )
        pov_stats.next_to(self.pov_label, RIGHT, buff=0.5)
        inc_stats.next_to(self.inc_label, RIGHT, buff=0.5)

        self.play(
            FadeIn(pov_stats, run_time=0.8),
            FadeIn(inc_stats, run_time=0.8),
        )
        self.wait(1.0)

        # ── Fade out formula, keep stats ─────────────────────────────────
        self.play(
            FadeOut(formula),
            FadeOut(formula_label),
            run_time=0.8,
        )
        self.wait(0.3)

        # ── Build unified z-score axis ─────────────────────────────────────
        z_axis = Axes(
            x_range=[-3, 3, 1],
            y_range=[0, 0.5, 1],
            x_length=10,
            y_length=0.5,
            axis_config={
                "include_numbers": True,
                "font_size": 18,
                "color": AMBER,
                "stroke_width": 2.0,
            },
            x_axis_config={
                "numbers_to_include": [-3, -2, -1, 0, 1, 2, 3],
                "label_direction": DOWN,
            },
            y_axis_config={"stroke_width": 0},
        )
        z_axis.shift(DOWN * 0.3)
        z_axis.set_opacity(0.0)

        z_label = make_text("Z-SCORE", font_size=18, color=AMBER)
        z_label.next_to(z_axis.get_right(), RIGHT, buff=0.2).shift(UP * 0.05)

        # ── Animate the unified axis appearing ─────────────────────────────
        self.play(
            z_axis.animate.set_opacity(0.8),
            FadeIn(z_label),
            run_time=1.5,
        )
        self.wait(0.5)

        # ── Compute z-scores ───────────────────────────────────────────────
        pov_zscores = [z_score(x, POVERTY_MEAN, POVERTY_STD) for x in POVERTY_VALUES]
        inc_zscores = [z_score(x, INCOME_MEAN, INCOME_STD) for x in INCOME_VALUES]

        # ── Animate dots sliding to z-score positions ──────────────────────
        # First, fade out the old axes
        self.play(
            self.pov_axes.animate.set_opacity(0.2),
            self.inc_axes.animate.set_opacity(0.2),
            FadeOut(self.pov_unit),
            FadeOut(self.inc_unit),
            run_time=1.0,
        )
        self.wait(0.3)

        # Transform poverty dots to z-score positions
        pov_new_dots = VGroup()
        pov_new_labels = VGroup()
        for i, (zval, old_dot) in enumerate(zip(pov_zscores, self.pov_dots)):
            new_pos = z_axis.c2p(zval, 0.3)
            new_dot = Dot(
                new_pos,
                color=AMBER,
                radius=0.09,
            )
            new_label = make_text(f"{zval:+.2f}", font_size=14, color=AMBER)
            new_label.next_to(new_dot, UP, buff=0.15)
            pov_new_dots.add(new_dot)
            pov_new_labels.add(new_label)

        inc_new_dots = VGroup()
        inc_new_labels = VGroup()
        for i, (zval, old_dot) in enumerate(zip(inc_zscores, self.inc_dots)):
            new_pos = z_axis.c2p(zval, 0.3)
            new_dot = Dot(
                new_pos,
                color=AMBER,
                radius=0.09,
            )
            new_label = make_text(f"{zval:+.2f}", font_size=14, color=AMBER)
            new_label.next_to(new_dot, UP, buff=0.15)
            inc_new_dots.add(new_dot)
            inc_new_labels.add(new_label)

        # Animate the transition: all dots slide to z-score positions
        self.play(
            *[ReplacementTransform(old_dot, new_dot, run_time=2.0)
              for old_dot, new_dot in zip(self.pov_dots, pov_new_dots)],
            *[FadeOut(lbl) for lbl in self.pov_value_labels],
            *[FadeIn(lbl) for lbl in pov_new_labels],
            *[ReplacementTransform(old_dot, new_dot, run_time=2.0)
              for old_dot, new_dot in zip(self.inc_dots, inc_new_dots)],
            *[FadeOut(lbl) for lbl in self.inc_value_labels],
            *[FadeIn(lbl) for lbl in inc_new_labels],
            run_time=2.5,
        )
        self.wait(1.0)

        # ── "Now comparable" callout ────────────────────────────────────────
        now_comp = make_text(
            "Now on the same scale — directly comparable",
            font_size=18, color=AMBER
        )
        now_comp.shift(DOWN * 3.1)
        self.play(FadeIn(now_comp), run_time=1.0)
        self.wait(1.5)

        # Store for next phase
        self.z_axis = z_axis
        self.z_label = z_label
        self.pov_new_dots = pov_new_dots
        self.pov_new_labels = pov_new_labels
        self.inc_new_dots = inc_new_dots
        self.inc_new_labels = inc_new_labels
        self.pov_stats = pov_stats
        self.inc_stats = inc_stats

        # ── Clean up ───────────────────────────────────────────────────────
        self.play(
            FadeOut(phase_title),
            FadeOut(now_comp),
            FadeOut(pov_stats),
            FadeOut(inc_stats),
            self.pov_label.animate.set_opacity(0.2),
            self.inc_label.animate.set_opacity(0.2),
            run_time=0.8,
        )
        self.wait(0.3)

    def phase3_bell_curve(self):
        """Overlay the standard normal distribution bell curve behind the
        standardized points."""

        # ── Phase title ────────────────────────────────────────────────────
        phase_title = make_text(
            "THE BELL CURVE", font_size=30, color=CURVE, weight=BOLD
        )
        phase_title.to_edge(UP, buff=0.6)
        self.play(Write(phase_title), run_time=1.5)
        self.wait(0.5)

        subtitle = make_text(
            "Standard Normal Distribution", font_size=18, color=DIM
        )
        subtitle.next_to(phase_title, DOWN, buff=0.15)
        self.play(FadeIn(subtitle), run_time=0.8)
        self.wait(0.5)

        # ── Draw the normal curve ──────────────────────────────────────────
        # Normal curve: f(x) = (1/sqrt(2*pi)) * exp(-x^2/2)
        def normal_pdf(x):
            return (1 / np.sqrt(2 * np.pi)) * np.exp(-x**2 / 2)

        # Scale to fit nicely on the z-axis
        curve = self.z_axis.plot(
            lambda x: normal_pdf(x) * 2.2,
            x_range=[-3.5, 3.5],
            color=CURVE,
            stroke_width=3,
        )
        curve.set_opacity(0.0)

        # Fill under the curve
        fill = self.z_axis.get_area(
            curve,
            x_range=(-3.5, 3.5),
            color=CURVE_FILL,
            opacity=0.12,
        )
        fill.set_opacity(0.0)

        self.play(
            curve.animate.set_opacity(0.8),
            fill.animate.set_opacity(0.12),
            run_time=2.0,
        )
        self.wait(0.5)

        # ── Add σ labels on the axis ───────────────────────────────────────
        sigma_labels = VGroup()
        for s in range(-3, 4):
            if s == 0:
                txt = "0 (μ)"
            else:
                txt = f"{s:+d}σ"
            lbl = make_text(txt, font_size=16, color=CURVE)
            lbl.next_to(self.z_axis.c2p(s, 0), DOWN, buff=0.35)
            sigma_labels.add(lbl)

        self.play(
            LaggedStart(
                *[FadeIn(lbl) for lbl in sigma_labels],
                lag_ratio=0.15,
                run_time=1.5,
            ),
        )
        self.wait(1.0)

        # ── Highlight where dots fall on the curve ─────────────────────────
        # Draw vertical dashed lines from each dot to the curve
        all_dots = VGroup(*self.pov_new_dots, *self.inc_new_dots)
        lines = VGroup()
        for dot in all_dots:
            x = self.z_axis.p2c(dot.get_center())[0]
            y_curve = normal_pdf(x) * 2.2
            curve_point = self.z_axis.c2p(x, y_curve)
            dot_point = self.z_axis.c2p(x, 0.3)
            line = DashedLine(
                curve_point,
                dot_point,
                color=AMBER,
                stroke_width=1,
                dash_length=0.05,
                stroke_opacity=0.5,
            )
            lines.add(line)

        self.play(
            LaggedStart(
                *[Create(line) for line in lines],
                lag_ratio=0.1,
                run_time=1.5,
            ),
        )
        self.wait(1.0)

        # ── Interpretation labels ──────────────────────────────────────────
        low_label = make_text("Below average", font_size=16, color=DIM)
        low_label.next_to(self.z_axis.c2p(-2, 0), DOWN, buff=0.9)

        avg_label = make_text("Average", font_size=16, color=CURVE)
        avg_label.next_to(self.z_axis.c2p(0, 0), DOWN, buff=0.9)

        high_label = make_text("Above average", font_size=16, color=DIM)
        high_label.next_to(self.z_axis.c2p(2, 0), DOWN, buff=0.9)

        self.play(
            FadeIn(low_label),
            FadeIn(avg_label),
            FadeIn(high_label),
            run_time=1.0,
        )
        self.wait(1.5)

        # Store
        self.phase_title = phase_title
        self.subtitle = subtitle
        self.curve = curve
        self.fill = fill
        self.lines = lines
        self.sigma_labels = sigma_labels
        self.low_label = low_label
        self.avg_label = avg_label
        self.high_label = high_label

    def phase4_insight(self):
        """Show the key insight: z-scores make different metrics comparable."""

        # ── Fade out everything except the bell curve and dots ─────────────
        self.play(
            FadeOut(self.phase_title),
            FadeOut(self.subtitle),
            FadeOut(self.sigma_labels),
            FadeOut(self.low_label),
            FadeOut(self.avg_label),
            FadeOut(self.high_label),
            FadeOut(self.lines),
            FadeOut(self.pov_label),
            FadeOut(self.inc_label),
            run_time=1.0,
        )
        self.wait(0.3)

        # ── Move the whole visualization up a bit to make room ─────────────
        everything = VGroup(
            self.z_axis, self.z_label, self.curve, self.fill,
            *self.pov_new_dots, *self.pov_new_labels,
            *self.inc_new_dots, *self.inc_new_labels,
        )
        self.play(
            everything.animate.shift(UP * 0.6).scale(0.85),
            run_time=1.5,
        )
        self.wait(0.5)

        # ── Key insight title ──────────────────────────────────────────────
        insight_title = make_text(
            "Z-SCORE NORMALIZATION", font_size=36, color=AMBER, weight=BOLD
        )
        insight_title.to_edge(UP, buff=0.4)
        self.play(Write(insight_title), run_time=1.5)
        self.wait(0.5)

        # ── Three key takeaways ────────────────────────────────────────────
        takeaways = VGroup(
            make_text(
                "1. Transforms any metric to a common scale",
                font_size=20, color=WHITE
            ),
            make_text(
                "2. Mean = 0, each unit = 1 standard deviation",
                font_size=20, color=WHITE
            ),
            make_text(
                "3. Apples-to-apples comparison across indicators",
                font_size=20, color=WHITE
            ),
        )
        takeaways.arrange(DOWN, buff=0.25, aligned_edge=LEFT)
        takeaways.shift(DOWN * 2.7)

        self.play(
            LaggedStart(
                *[FadeIn(t, shift=RIGHT * 0.3) for t in takeaways],
                lag_ratio=0.3,
                run_time=2.5,
            ),
        )
        self.wait(2.0)

        # ── Final reveal: formula reminder ─────────────────────────────────
        formula_final = MathTex(
            r"z = \frac{x - \mu}{\sigma}",
            font_size=36,
            color=AMBER,
        )
        formula_final.shift(DOWN * 1.6)
        self.play(Write(formula_final), run_time=1.5)
        self.wait(2.0)

        # ── Clean exit ─────────────────────────────────────────────────────
        self.play(
            FadeOut(Group(*self.mobjects)),
            run_time=1.0,
        )
        self.wait(0.3)