"""
Bootstrap Confidence Intervals Animation
Policy Data Infrastructure — ADR-007 stats animation series

Narrative: How do we know if a sample statistic is reliable? Bootstrap resampling
builds a sampling distribution by drawing 1,000s of resamples from the original
data. The 2.5th and 97.5th percentiles become the 95% confidence interval.

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
HIST = "#8b5cf6"          # histogram fill
CI_COLOR = "#f59e0b"      # CI brackets
SAMPLE_COLOR = "#06b6d4"  # cyan — original sample
RESAMPLE_COLOR = "#a78bfa"  # light violet — resamples
MONO = "Menlo"

# ── Data ─────────────────────────────────────────────────────────────────────
# Simulated income data (20 observations, $K)
np.random.seed(42)
POPULATION = np.random.normal(55, 15, 1000)  # hidden population
SAMPLE = np.random.choice(POPULATION, size=20, replace=False)
SAMPLE_MEAN = np.mean(SAMPLE)

# ── Helpers ──────────────────────────────────────────────────────────────────
def make_text(s, font_size=24, color=WHITE, weight=None):
    kwargs = {"font_size": font_size, "color": color, "font": MONO}
    if weight:
        kwargs["weight"] = weight
    return Text(s, **kwargs)


class BootstrapCI(Scene):
    """Bootstrap CI explainer: sample → resample → histogram → CI brackets."""

    def construct(self):
        self.camera.background_color = BG

        self.phase1_sample()
        self.phase2_resample()
        self.phase3_histogram()
        self.phase4_insight()

    def phase1_sample(self):
        """Show the original sample and the problem of uncertainty."""

        title = make_text("THE PROBLEM", font_size=36, color=WHITE, weight=BOLD)
        title.to_edge(UP, buff=0.6)
        self.play(Write(title), run_time=1.5)
        self.wait(0.5)

        subtitle = make_text(
            "Your sample gives an estimate. How reliable is it?",
            font_size=20, color=DIM,
        )
        subtitle.next_to(title, DOWN, buff=0.25)
        self.play(FadeIn(subtitle), run_time=1.0)
        self.wait(1.0)

        # ── Show the sample as dots on a number line ────────────────────────
        num_line = NumberLine(
            x_range=[20, 90, 10],
            length=10,
            color=SUBTLE,
            include_numbers=True,
            font_size=16,
        )
        num_line.shift(DOWN * 0.2)

        num_label = make_text("INCOME ($K)", font_size=18, color=GRAY)
        num_label.next_to(num_line.get_right(), RIGHT, buff=0.3)

        # Plot sample dots
        sample_dots = VGroup()
        sample_labels = VGroup()
        # Use a subset showing on the line
        display_sample = sorted(SAMPLE)[:15]  # Show 15 dots
        y_positions = [0.3, 0.6, 0.9, 0.3, 0.6, 0.9, 0.3, 0.6, 0.9, 0.3, 0.6, 0.9, 0.3, 0.6, 0.9]

        for val, y_off in zip(display_sample, y_positions):
            dot = Dot(
                num_line.n2p(val) + UP * y_off,
                color=SAMPLE_COLOR, radius=0.06,
            )
            sample_dots.add(dot)

        self.play(Create(num_line), FadeIn(num_label), run_time=1.5)
        self.wait(0.5)

        self.play(
            LaggedStart(
                *[GrowFromCenter(d) for d in sample_dots],
                lag_ratio=0.1,
                run_time=2.0,
            ),
        )
        self.wait(0.5)

        # ── Mean indicator ──────────────────────────────────────────────────
        mean_line = DashedLine(
            start=num_line.n2p(SAMPLE_MEAN) + UP * 0.1,
            end=num_line.n2p(SAMPLE_MEAN) + DOWN * 0.2,
            color=AMBER, stroke_width=2, dash_length=0.1,
        )
        mean_label = make_text(
            f"Mean = ${SAMPLE_MEAN:.1f}K", font_size=18, color=AMBER,
        )
        mean_label.next_to(mean_line, UP, buff=0.15)

        self.play(Create(mean_line), FadeIn(mean_label), run_time=1.0)
        self.wait(1.0)

        # ── Uncertainty question ────────────────────────────────────────────
        question = make_text(
            "But what if we had drawn a different sample?",
            font_size=18, color=DIM,
        )
        question.shift(DOWN * 1.5)
        self.play(FadeIn(question), run_time=1.0)
        self.wait(1.5)

        self.play(
            FadeOut(title), FadeOut(subtitle), FadeOut(question),
            run_time=0.8,
        )
        self.wait(0.3)

        self.num_line = num_line
        self.num_label = num_label
        self.sample_dots = sample_dots
        self.mean_label = mean_label
        self.mean_line = mean_line

    def phase2_resample(self):
        """Show resampling: draw with replacement, compute new means."""

        phase_title = make_text("BOOTSTRAP RESAMPLING", font_size=30, color=AMBER, weight=BOLD)
        phase_title.to_edge(UP, buff=0.6)
        self.play(Write(phase_title), run_time=1.5)
        self.wait(0.5)

        explanation = make_text(
            "Draw 1,000 new samples (with replacement) from the original.\n"
            "Compute the mean of each. Build a sampling distribution.",
            font_size=16, color=DIM,
        )
        explanation.next_to(phase_title, DOWN, buff=0.3)
        self.play(FadeIn(explanation), run_time=1.5)
        self.wait(1.0)

        # ── Show concept of resampling ──────────────────────────────────────
        # Visual: original sample dots, arrows showing "resample → new sample"
        resample_label = make_text(
            "Resample 1 → Resample 2 → ... → Resample 1000",
            font_size=16, color=RESAMPLE_COLOR,
        )
        resample_label.shift(UP * 0.5)
        self.play(FadeIn(resample_label), run_time=1.0)
        self.wait(0.5)

        # ── Animate a few resampled means appearing ─────────────────────────
        # Precompute bootstrap means
        np.random.seed(123)
        n_bootstrap = 200  # Show 200 for preview, 1000 for final
        bootstrap_means = []
        for _ in range(n_bootstrap):
            resample = np.random.choice(SAMPLE, size=len(SAMPLE), replace=True)
            bootstrap_means.append(np.mean(resample))

        # Show first few resampled means as dots on the number line
        resample_dots = VGroup()
        for i, m in enumerate(bootstrap_means[:12]):
            y_jitter = 0.3 + 0.15 * (i % 3)
            dot = Dot(
                self.num_line.n2p(m) + UP * y_jitter,
                color=RESAMPLE_COLOR, radius=0.04,
                fill_opacity=0.6,
            )
            resample_dots.add(dot)

        self.play(
            LaggedStart(
                *[GrowFromCenter(d) for d in resample_dots],
                lag_ratio=0.08,
                run_time=2.0,
            ),
        )
        self.wait(1.0)

        # ── Counter ─────────────────────────────────────────────────────────
        counter = make_text(
            f"{n_bootstrap} resamples", font_size=20, color=AMBER,
        )
        counter.shift(DOWN * 0.3)
        self.play(FadeIn(counter), run_time=0.8)
        self.wait(1.0)

        self.play(
            FadeOut(phase_title), FadeOut(explanation), FadeOut(resample_label),
            FadeOut(resample_dots), FadeOut(counter),
            run_time=0.8,
        )
        self.wait(0.3)

        self.bootstrap_means = bootstrap_means

    def phase3_histogram(self):
        """Build a histogram of bootstrap means. CI brackets emerge."""

        phase_title = make_text("SAMPLING DISTRIBUTION", font_size=30, color=HIST, weight=BOLD)
        phase_title.to_edge(UP, buff=0.6)
        self.play(Write(phase_title), run_time=1.5)
        self.wait(0.5)

        # ── Build histogram from bootstrap means ────────────────────────────
        # Use all 1000 bootstrap means
        np.random.seed(123)
        n_bootstrap = 1000
        bootstrap_means = []
        for _ in range(n_bootstrap):
            resample = np.random.choice(SAMPLE, size=len(SAMPLE), replace=True)
            bootstrap_means.append(np.mean(resample))
        bootstrap_means = np.array(bootstrap_means)

        ci_lower = np.percentile(bootstrap_means, 2.5)
        ci_upper = np.percentile(bootstrap_means, 97.5)

        # ── Draw histogram axes ─────────────────────────────────────────────
        hist_axes = Axes(
            x_range=[35, 75, 5],
            y_range=[0, 180, 50],
            x_length=9,
            y_length=4,
            axis_config={
                "include_numbers": True,
                "font_size": 16,
                "color": SUBTLE,
                "stroke_width": 1.5,
            },
            x_axis_config={
                "numbers_to_include": [35, 45, 55, 65, 75],
            },
        )
        hist_axes.shift(DOWN * 0.3)

        x_label = make_text("Bootstrap Mean Income ($K)", font_size=18, color=GRAY)
        x_label.next_to(hist_axes.x_axis, DOWN, buff=0.4)

        y_label = make_text("Count", font_size=18, color=GRAY)
        y_label.rotate(PI / 2)
        y_label.next_to(hist_axes.y_axis, LEFT, buff=0.4)

        self.play(
            Create(hist_axes), FadeIn(x_label), FadeIn(y_label),
            run_time=1.5,
        )
        self.wait(0.5)

        # ── Build histogram bars ────────────────────────────────────────────
        bin_edges = np.linspace(35, 75, 21)
        hist_counts, _ = np.histogram(bootstrap_means, bins=bin_edges)
        bar_width = 9.0 / 20

        bars = VGroup()
        for i, count in enumerate(hist_counts):
            if count > 0:
                x_center = bin_edges[i] + bar_width / 2
                bar = Rectangle(
                    width=bar_width * 0.85,
                    height=hist_axes.c2p(0, count)[1] - hist_axes.c2p(0, 0)[1],
                    fill_color=HIST,
                    fill_opacity=0.6,
                    stroke_color=HIST,
                    stroke_width=1,
                    stroke_opacity=0.3,
                )
                bar.move_to(hist_axes.c2p(x_center, count / 2))
                bars.add(bar)

        # Animate bars appearing as if building up counts
        self.play(
            LaggedStart(
                *[FadeIn(bar) for bar in bars],
                lag_ratio=0.06,
                run_time=3.0,
            ),
        )
        self.wait(1.0)

        # ── CI brackets ─────────────────────────────────────────────────────
        ci_lower_line = DashedLine(
            start=hist_axes.c2p(ci_lower, 0),
            end=hist_axes.c2p(ci_lower, max(hist_counts) * 1.1),
            color=CI_COLOR, stroke_width=2.5, dash_length=0.1,
        )
        ci_upper_line = DashedLine(
            start=hist_axes.c2p(ci_upper, 0),
            end=hist_axes.c2p(ci_upper, max(hist_counts) * 1.1),
            color=CI_COLOR, stroke_width=2.5, dash_length=0.1,
        )

        ci_lower_label = make_text(f"${ci_lower:.1f}K", font_size=16, color=CI_COLOR)
        ci_lower_label.next_to(ci_lower_line, UP, buff=0.15)

        ci_upper_label = make_text(f"${ci_upper:.1f}K", font_size=16, color=CI_COLOR)
        ci_upper_label.next_to(ci_upper_line, UP, buff=0.15)

        ci_title = make_text("95% CI", font_size=18, color=CI_COLOR)
        ci_title.next_to(hist_axes, DOWN, buff=0.5)

        self.play(
            Create(ci_lower_line), FadeIn(ci_lower_label),
            Create(ci_upper_line), FadeIn(ci_upper_label),
            FadeIn(ci_title),
            run_time=2.0,
        )
        self.wait(1.5)

        # ── Original mean marker ────────────────────────────────────────────
        orig_marker = DashedLine(
            start=hist_axes.c2p(SAMPLE_MEAN, 0),
            end=hist_axes.c2p(SAMPLE_MEAN, max(hist_counts) * 0.9),
            color=SAMPLE_COLOR, stroke_width=2, dash_length=0.08,
        )
        orig_label = make_text(f"Original: ${SAMPLE_MEAN:.1f}K", font_size=16, color=SAMPLE_COLOR)
        orig_label.next_to(orig_marker, UP, buff=0.1)

        self.play(Create(orig_marker), FadeIn(orig_label), run_time=1.0)
        self.wait(1.5)

        self.play(
            FadeOut(phase_title),
            run_time=0.8,
        )
        self.wait(0.3)

        self.hist_axes = hist_axes
        self.hist_bars = bars
        self.ci_lower_line = ci_lower_line
        self.ci_upper_line = ci_upper_line
        self.orig_marker = orig_marker

    def phase4_insight(self):
        """Key insight: bootstrap builds CIs from data alone."""

        insight_title = make_text(
            "BOOTSTRAP CONFIDENCE INTERVALS",
            font_size=30, color=AMBER, weight=BOLD,
        )
        insight_title.to_edge(UP, buff=0.4)
        self.play(Write(insight_title), run_time=1.5)
        self.wait(0.5)

        takeaways = VGroup(
            make_text(
                "1. Resample with replacement to simulate new draws",
                font_size=20, color=WHITE,
            ),
            make_text(
                "2. Build a sampling distribution of the statistic",
                font_size=20, color=WHITE,
            ),
            make_text(
                "3. 2.5th and 97.5th percentiles → 95% CI",
                font_size=20, color=WHITE,
            ),
        )
        takeaways.arrange(DOWN, buff=0.25, aligned_edge=LEFT)
        takeaways.shift(UP * 0.1)

        self.play(
            LaggedStart(
                *[FadeIn(t, shift=RIGHT * 0.3) for t in takeaways],
                lag_ratio=0.3,
                run_time=2.5,
            ),
        )
        self.wait(2.0)

        formula_final = make_text(
            "No assumptions about the underlying distribution required.",
            font_size=20, color=AMBER,
        )
        formula_final.next_to(takeaways, DOWN, buff=0.5)
        self.play(FadeIn(formula_final), run_time=1.5)
        self.wait(2.0)

        self.play(
            FadeOut(Group(*self.mobjects)),
            run_time=1.0,
        )
        self.wait(0.3)
