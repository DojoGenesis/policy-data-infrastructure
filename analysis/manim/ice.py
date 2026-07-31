"""
ICE (Index of Concentration at Extremes) Animation
Policy Data Infrastructure — ADR-007 stats animation series

Narrative: ICE measures spatial polarization — how concentrated privilege and
deprivation are at opposite extremes. Scale: -1 (extreme deprivation) to +1
(extreme privilege). Walk through the formula and show bars pulling apart.

Style: Night Shift palette — dark background (#0a0a0f), amber accent, system font.
No voiceover — text labels carry the explanation.
Target: 60-90 seconds, single scene with four phases.
"""

from manim import *
import numpy as np

# ── Palette ──────────────────────────────────────────────────────────────────
BG = "#0a0a0f"
AMBER = "#f59e0b"
GRAY = "#6b7280"
SUBTLE = "#3f3f46"
WHITE = "#e4e4e7"
DIM = "#52525b"
DEPRIVED = "#ef4444"      # red — deprivation
PRIVILEGED = "#3b82f6"    # blue — privilege
MIDDLE = "#71717a"        # neutral middle
MIDPOINT = "#a855f7"      # purple — zero point
MONO = "Menlo"

# ── Helpers ──────────────────────────────────────────────────────────────────
def make_text(s, font_size=24, color=WHITE, weight=None):
    kwargs = {"font_size": font_size, "color": color, "font": MONO}
    if weight:
        kwargs["weight"] = weight
    return Text(s, **kwargs)


class ICE(Scene):
    """ICE explainer: polarization bars → formula → scale → insight."""

    def construct(self):
        self.camera.background_color = BG

        self.phase1_problem()
        self.phase2_formula()
        self.phase3_bars()
        self.phase4_insight()

    def phase1_problem(self):
        """Introduce the concept: spatial polarization."""

        title = make_text("THE PROBLEM", font_size=36, color=WHITE, weight=BOLD)
        title.to_edge(UP, buff=0.6)
        self.play(Write(title), run_time=1.5)
        self.wait(0.5)

        subtitle = make_text(
            "Some places concentrate privilege. Others concentrate deprivation.",
            font_size=20, color=DIM,
        )
        subtitle.next_to(title, DOWN, buff=0.25)
        self.play(FadeIn(subtitle), run_time=1.0)
        self.wait(1.0)

        # ── Show two simple bars anchored by total ─────────────────────────
        # Total population bar (anchor)
        total_bar = Rectangle(
            width=6.0, height=0.6,
            fill_color=MIDDLE, fill_opacity=0.5,
            stroke_color=SUBTLE, stroke_width=1.5,
        )
        total_bar.move_to(UP * 1.0)
        total_label = make_text("TOTAL POPULATION", font_size=18, color=GRAY)
        total_label.next_to(total_bar, UP, buff=0.2)

        # Privileged bar (right side, blue)
        priv_bar = Rectangle(
            width=1.8, height=0.6,
            fill_color=PRIVILEGED, fill_opacity=0.8,
            stroke_color=PRIVILEGED, stroke_width=1.5,
        )
        priv_bar.move_to(UP * 1.0 + RIGHT * 2.1)
        priv_label = make_text("Privileged", font_size=16, color=PRIVILEGED)
        priv_label.next_to(priv_bar, UP, buff=0.2)

        # Deprived bar (left side, red)
        depr_bar = Rectangle(
            width=1.5, height=0.6,
            fill_color=DEPRIVED, fill_opacity=0.8,
            stroke_color=DEPRIVED, stroke_width=1.5,
        )
        depr_bar.move_to(UP * 1.0 + LEFT * 2.25)
        depr_label = make_text("Deprived", font_size=16, color=DEPRIVED)
        depr_label.next_to(depr_bar, UP, buff=0.2)

        # Middle (everyone else)
        mid_label = make_text("Middle", font_size=14, color=DIM)
        mid_label.next_to(total_bar, DOWN, buff=0.25)

        self.play(
            FadeIn(total_bar), FadeIn(total_label),
            run_time=1.0,
        )
        self.wait(0.5)

        self.play(
            FadeIn(priv_bar), FadeIn(priv_label),
            FadeIn(depr_bar), FadeIn(depr_label),
            FadeIn(mid_label),
            run_time=1.5,
        )
        self.wait(1.0)

        # ── Question ────────────────────────────────────────────────────────
        question = make_text(
            "How polarized is this place?",
            font_size=22, color=WHITE,
        )
        question.shift(DOWN * 1.2)
        self.play(Write(question), run_time=1.0)
        self.wait(1.5)

        self.question = question
        self.total_bar = total_bar
        self.total_label = total_label
        self.priv_bar = priv_bar
        self.priv_label = priv_label
        self.depr_bar = depr_bar
        self.depr_label = depr_label
        self.mid_label = mid_label
        self.title = title
        self.subtitle = subtitle

        self.play(
            FadeOut(title), FadeOut(subtitle), FadeOut(question),
            run_time=0.8,
        )
        self.wait(0.3)

    def phase2_formula(self):
        """Show the ICE formula and walk through a calculation."""

        phase_title = make_text("ICE FORMULA", font_size=30, color=AMBER, weight=BOLD)
        phase_title.to_edge(UP, buff=0.6)
        self.play(Write(phase_title), run_time=1.5)
        self.wait(0.5)

        # ── Formula ─────────────────────────────────────────────────────────
        formula = MathTex(
            r"\text{ICE} = \frac{\text{Privileged} - \text{Deprived}}{\text{Total}}",
            font_size=36, color=WHITE,
        )
        formula.shift(UP * 0.5)
        self.play(Write(formula), run_time=2.0)
        self.wait(1.0)

        # ── Show example values ─────────────────────────────────────────────
        priv_val = make_text("Privileged = 30%", font_size=20, color=PRIVILEGED)
        depr_val = make_text("Deprived   = 20%", font_size=20, color=DEPRIVED)
        total_val = make_text("Total      = 100%", font_size=20, color=GRAY)

        vals = VGroup(priv_val, depr_val, total_val)
        vals.arrange(DOWN, buff=0.15, aligned_edge=LEFT)
        vals.shift(DOWN * 0.6)

        self.play(
            LaggedStart(
                FadeIn(priv_val, shift=RIGHT * 0.3),
                FadeIn(depr_val, shift=RIGHT * 0.3),
                FadeIn(total_val, shift=RIGHT * 0.3),
                lag_ratio=0.3,
                run_time=1.5,
            ),
        )
        self.wait(1.0)

        # ── Compute and show result ─────────────────────────────────────────
        result = MathTex(
            r"\text{ICE} = \frac{30 - 20}{100} = \frac{10}{100} = +0.10",
            font_size=30, color=AMBER,
        )
        result.shift(DOWN * 1.8)
        self.play(Write(result), run_time=2.0)
        self.wait(1.5)

        # ── Scale annotation ────────────────────────────────────────────────
        scale = make_text(
            "Scale: -1 (extreme deprivation)  to  +1 (extreme privilege)",
            font_size=16, color=DIM,
        )
        scale.next_to(result, DOWN, buff=0.4)
        self.play(FadeIn(scale), run_time=1.0)
        self.wait(1.0)

        self.play(
            FadeOut(formula), FadeOut(vals), FadeOut(result), FadeOut(scale),
            run_time=0.8,
        )
        self.wait(0.3)

        self.phase_title = phase_title

    def phase3_bars(self):
        """Animate bars pulling apart showing -1, 0, +1 scenarios."""

        self.play(
            FadeOut(self.phase_title),
            run_time=0.5,
        )

        phase_title = make_text("ICE SCALE", font_size=30, color=AMBER, weight=BOLD)
        phase_title.to_edge(UP, buff=0.6)
        self.play(Write(phase_title), run_time=1.5)
        self.wait(0.5)

        # ── Number line ─────────────────────────────────────────────────────
        num_line = NumberLine(
            x_range=[-1, 1, 0.5],
            length=10,
            color=SUBTLE,
            include_numbers=True,
            font_size=18,
            numbers_with_elongated_ticks=[],
        )
        num_line.shift(DOWN * 2.8)
        self.play(Create(num_line), run_time=1.5)
        self.wait(0.5)

        # ── Zero marker ────────────────────────────────────────────────────
        zero_label = make_text("0 = balanced", font_size=16, color=MIDPOINT)
        zero_label.next_to(num_line.n2p(0), DOWN, buff=0.5)

        neg_label = make_text("-1 = deprived", font_size=16, color=DEPRIVED)
        neg_label.next_to(num_line.n2p(-1), DOWN, buff=0.5)

        pos_label = make_text("+1 = privileged", font_size=16, color=PRIVILEGED)
        pos_label.next_to(num_line.n2p(1), DOWN, buff=0.5)

        self.play(
            FadeIn(zero_label), FadeIn(neg_label), FadeIn(pos_label),
            run_time=1.0,
        )
        self.wait(1.0)

        # ── Three scenarios with bars ──────────────────────────────────────
        bar_height = 0.5

        # Scenario 1: ICE = -0.6 (deprived)
        s1_bar = Rectangle(
            width=3.0, height=bar_height,
            fill_color=DEPRIVED, fill_opacity=0.7,
            stroke_color=DEPRIVED, stroke_width=1.5,
        )
        s1_bar.move_to(num_line.n2p(-0.6)).shift(UP * 0.5)
        s1_label = make_text("-0.6", font_size=18, color=DEPRIVED)
        s1_label.next_to(s1_bar, UP, buff=0.15)

        # Scenario 2: ICE = 0 (balanced)
        s2_bar = Rectangle(
            width=1.8, height=bar_height,
            fill_color=MIDPOINT, fill_opacity=0.7,
            stroke_color=MIDPOINT, stroke_width=1.5,
        )
        s2_bar.move_to(num_line.n2p(0)).shift(UP * 1.5)
        s2_label = make_text("0", font_size=18, color=MIDPOINT)
        s2_label.next_to(s2_bar, UP, buff=0.15)

        # Scenario 3: ICE = +0.5 (privileged)
        s3_bar = Rectangle(
            width=2.5, height=bar_height,
            fill_color=PRIVILEGED, fill_opacity=0.7,
            stroke_color=PRIVILEGED, stroke_width=1.5,
        )
        s3_bar.move_to(num_line.n2p(0.5)).shift(DOWN * 0.5)
        s3_label = make_text("+0.5", font_size=18, color=PRIVILEGED)
        s3_label.next_to(s3_bar, UP, buff=0.15)

        # Arrow from zero
        arrow_left = Arrow(
            start=num_line.n2p(0), end=num_line.n2p(-0.55),
            color=DEPRIVED, buff=0, stroke_width=2,
            tip_length=0.2,
        )
        arrow_right = Arrow(
            start=num_line.n2p(0), end=num_line.n2p(0.45),
            color=PRIVILEGED, buff=0, stroke_width=2,
            tip_length=0.2,
        )

        self.play(
            LaggedStart(
                FadeIn(s1_bar), FadeIn(s1_label),
                lag_ratio=0.3, run_time=1.0,
            ),
        )
        self.wait(0.3)

        self.play(
            LaggedStart(
                Create(arrow_left),
                lag_ratio=0, run_time=0.8,
            ),
        )
        self.wait(0.5)

        self.play(
            LaggedStart(
                FadeIn(s2_bar), FadeIn(s2_label),
                lag_ratio=0.3, run_time=1.0,
            ),
        )
        self.wait(0.3)

        self.play(
            LaggedStart(
                FadeIn(s3_bar), FadeIn(s3_label),
                lag_ratio=0.3, run_time=1.0,
            ),
        )
        self.wait(0.3)

        self.play(
            LaggedStart(
                Create(arrow_right),
                lag_ratio=0, run_time=0.8,
            ),
        )
        self.wait(1.0)

        self.play(
            FadeOut(phase_title), FadeOut(zero_label), FadeOut(neg_label),
            FadeOut(pos_label),
            run_time=0.8,
        )
        self.wait(0.3)

        self.num_line = num_line
        self.s1_bar = s1_bar
        self.s1_label = s1_label
        self.s2_bar = s2_bar
        self.s2_label = s2_label
        self.s3_bar = s3_bar
        self.s3_label = s3_label
        self.arrow_left = arrow_left
        self.arrow_right = arrow_right

    def phase4_insight(self):
        """Key insight: ICE measures spatial polarization."""

        # Fade out bars and arrows
        self.play(
            FadeOut(self.s1_bar), FadeOut(self.s1_label),
            FadeOut(self.s2_bar), FadeOut(self.s2_label),
            FadeOut(self.s3_bar), FadeOut(self.s3_label),
            FadeOut(self.arrow_left), FadeOut(self.arrow_right),
            run_time=0.8,
        )

        # ── Keep number line, add callout ───────────────────────────────────
        insight_title = make_text(
            "INDEX OF CONCENTRATION AT EXTREMES",
            font_size=30, color=AMBER, weight=BOLD,
        )
        insight_title.to_edge(UP, buff=0.4)
        self.play(Write(insight_title), run_time=1.5)
        self.wait(0.5)

        takeaways = VGroup(
            make_text(
                "1. Measures spatial polarization on a -1 to +1 scale",
                font_size=20, color=WHITE,
            ),
            make_text(
                "2. Subtracts deprived from privileged, divided by total",
                font_size=20, color=WHITE,
            ),
            make_text(
                "3. Used across race, income, education dimensions",
                font_size=20, color=WHITE,
            ),
        )
        takeaways.arrange(DOWN, buff=0.25, aligned_edge=LEFT)
        takeaways.shift(DOWN * 0.3)

        self.play(
            LaggedStart(
                *[FadeIn(t, shift=RIGHT * 0.3) for t in takeaways],
                lag_ratio=0.3,
                run_time=2.5,
            ),
        )
        self.wait(2.0)

        # ── Final formula ───────────────────────────────────────────────────
        formula_final = MathTex(
            r"\text{ICE} = \frac{\text{Privileged} - \text{Deprived}}{\text{Total}}",
            font_size=30, color=AMBER,
        )
        formula_final.next_to(takeaways, DOWN, buff=0.5)
        self.play(Write(formula_final), run_time=1.5)
        self.wait(2.0)

        self.play(
            FadeOut(Group(*self.mobjects)),
            run_time=1.0,
        )
        self.wait(0.3)
