"""
Quantile Classification Animation
Policy Data Infrastructure — ADR-007 stats animation series

Narrative: Why arbitrary tier cutoffs (e.g., "low/medium/high") hide important
patterns. Quantile classification uses data-driven breaks — each tier contains
the same number of observations. Dots on a number line snap into 5 color bands.

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
Q1 = "#06b6d4"    # cyan — quintile 1 (lowest)
Q2 = "#22c55e"    # green
Q3 = "#f59e0b"    # amber (middle)
Q4 = "#f97316"    # orange
Q5 = "#ef4444"    # red (highest)
MONO = "Menlo"

# ── Helpers ──────────────────────────────────────────────────────────────────
def make_text(s, font_size=24, color=WHITE, weight=None):
    kwargs = {"font_size": font_size, "color": color, "font": MONO}
    if weight:
        kwargs["weight"] = weight
    return Text(s, **kwargs)


class QuantileClassification(Scene):
    """Quantile classification: arbitrary breaks → data-driven quintiles."""

    def construct(self):
        self.camera.background_color = BG

        self.phase1_problem()
        self.phase2_sort()
        self.phase3_bands()
        self.phase4_insight()

    def phase1_problem(self):
        """Show dots on a number line with arbitrary 'low/med/high' breaks."""

        title = make_text("THE PROBLEM", font_size=36, color=WHITE, weight=BOLD)
        title.to_edge(UP, buff=0.6)
        self.play(Write(title), run_time=1.5)
        self.wait(0.5)

        subtitle = make_text(
            "Arbitrary cutoffs hide true patterns in the data.",
            font_size=20, color=DIM,
        )
        subtitle.next_to(title, DOWN, buff=0.25)
        self.play(FadeIn(subtitle), run_time=1.0)
        self.wait(1.0)

        # ── Generate data points ────────────────────────────────────────────
        np.random.seed(42)
        values = np.concatenate([
            np.random.normal(10, 5, 15),     # low cluster
            np.random.normal(35, 8, 25),     # middle cluster
            np.random.normal(65, 10, 20),    # high cluster
            np.random.normal(90, 4, 10),     # very high cluster
        ])
        values = np.clip(values, 0, 100)

        # ── Number line ─────────────────────────────────────────────────────
        num_line = NumberLine(
            x_range=[0, 100, 20],
            length=12,
            color=SUBTLE,
            include_numbers=True,
            font_size=16,
        )
        num_line.shift(DOWN * 0.3)

        self.play(Create(num_line), run_time=1.5)
        self.wait(0.5)

        # ── Plot dots (all gray) ───────────────────────────────────────────
        dots = VGroup()
        for val in values:
            y_jitter = np.random.uniform(0.2, 1.0)
            dot = Dot(
                num_line.n2p(val) + UP * y_jitter,
                color=GRAY, radius=0.05,
                fill_opacity=0.7,
            )
            dots.add(dot)

        self.play(
            LaggedStart(
                *[GrowFromCenter(d) for d in dots],
                lag_ratio=0.03,
                run_time=2.5,
            ),
        )
        self.wait(1.0)

        # ── Show arbitrary breaks ───────────────────────────────────────────
        break_33 = DashedLine(
            start=num_line.n2p(33) + UP * 0.1,
            end=num_line.n2p(33) + DOWN * 0.8,
            color=GRAY, stroke_width=2, dash_length=0.15,
        )
        break_66 = DashedLine(
            start=num_line.n2p(66) + UP * 0.1,
            end=num_line.n2p(66) + DOWN * 0.8,
            color=GRAY, stroke_width=2, dash_length=0.15,
        )

        low_label = make_text("LOW", font_size=16, color=GRAY)
        low_label.next_to(num_line.n2p(16.5), DOWN, buff=0.6)

        med_label = make_text("MEDIUM", font_size=16, color=GRAY)
        med_label.next_to(num_line.n2p(49.5), DOWN, buff=0.6)

        high_label = make_text("HIGH", font_size=16, color=GRAY)
        high_label.next_to(num_line.n2p(83), DOWN, buff=0.6)

        self.play(
            Create(break_33), Create(break_66),
            FadeIn(low_label), FadeIn(med_label), FadeIn(high_label),
            run_time=2.0,
        )
        self.wait(1.5)

        # ── Problem callout ─────────────────────────────────────────────────
        problem = make_text(
            "Arbitrary: 17 dots in \"low\", 48 in \"high\" — misleading!",
            font_size=18, color=DIM,
        )
        problem.to_edge(DOWN, buff=0.6)
        self.play(FadeIn(problem), run_time=1.0)
        self.wait(1.5)

        self.play(
            FadeOut(title), FadeOut(subtitle), FadeOut(problem),
            FadeOut(break_33), FadeOut(break_66),
            FadeOut(low_label), FadeOut(med_label), FadeOut(high_label),
            run_time=0.8,
        )
        self.wait(0.3)

        self.num_line = num_line
        self.dots = dots
        self.values = values

    def phase2_sort(self):
        """Animate dots sorting from left to right, showing the data distribution."""

        phase_title = make_text("SORT & COUNT", font_size=30, color=AMBER, weight=BOLD)
        phase_title.to_edge(UP, buff=0.6)
        self.play(Write(phase_title), run_time=1.5)
        self.wait(0.5)

        sorted_vals = np.sort(self.values)
        n = len(sorted_vals)
        n_per_quintile = n // 5

        # Create new dots at sorted positions
        sorted_dots = VGroup()
        sorted_positions = []
        for i, val in enumerate(sorted_vals):
            y_pos = 0.3 + 0.6 * (i % 5) / 5  # stack in small rows
            pos = self.num_line.n2p(val) + UP * y_pos
            sorted_positions.append(pos)
            dot = Dot(
                pos,
                color=GRAY, radius=0.05,
                fill_opacity=0.7,
            )
            sorted_dots.add(dot)

        # Animate dots moving to sorted positions
        self.play(
            *[d.animate.move_to(pos) for d, pos in zip(self.dots, sorted_positions)],
            run_time=2.5,
        )
        self.wait(1.0)

        # ── Quintile markers ────────────────────────────────────────────────
        quintile_breaks = []
        break_labels = []
        for k in range(1, 5):
            idx = k * n_per_quintile
            break_val = sorted_vals[min(idx, n - 1)]
            q_break = DashedLine(
                start=self.num_line.n2p(break_val) + UP * 0.1,
                end=self.num_line.n2p(break_val) + DOWN * 0.6,
                color=AMBER, stroke_width=2, dash_length=0.12,
            )
            quintile_breaks.append(q_break)

            q_label = make_text(f"Q{k}", font_size=14, color=AMBER)
            q_label.next_to(q_break, DOWN, buff=0.35)
            break_labels.append(q_label)

        self.play(
            LaggedStart(
                *[Create(q) for q in quintile_breaks],
                *[FadeIn(l) for l in break_labels],
                lag_ratio=0.2, run_time=2.0,
            ),
        )
        self.wait(1.5)

        # Store
        self.sorted_vals = sorted_vals
        self.n_per_quintile = n_per_quintile
        self.quintile_breaks = quintile_breaks
        self.break_labels = break_labels

        self.play(
            FadeOut(phase_title),
            run_time=0.8,
        )
        self.wait(0.3)

    def phase3_bands(self):
        """Color-code dots into 5 quantile bands. Show the data-driven beauty."""

        phase_title = make_text("DATA-DRIVEN BREAKS", font_size=30, color=AMBER, weight=BOLD)
        phase_title.to_edge(UP, buff=0.6)
        self.play(Write(phase_title), run_time=1.5)
        self.wait(0.5)

        quintile_colors = [Q1, Q2, Q3, Q4, Q5]
        quintile_names = ["Lowest", "Low", "Middle", "High", "Highest"]

        # Color each dot by its quintile
        sorted_vals = self.sorted_vals
        n_per = self.n_per_quintile
        n = len(sorted_vals)

        # Rebuild colored dot positions
        all_colored_dots = VGroup()
        quintile_bands = VGroup()

        for qi in range(5):
            start_idx = qi * n_per
            end_idx = min((qi + 1) * n_per, n)
            if qi == 4:
                end_idx = n  # last quintile gets remainder

            band_dots = VGroup()
            for i in range(start_idx, end_idx):
                y_pos = 0.3 + 0.6 * (i % 5) / 5
                pos = self.num_line.n2p(sorted_vals[i]) + UP * y_pos
                # Transition existing dot
                if i < len(self.dots):
                    self.dots[i].set_color(quintile_colors[qi])
                    self.dots[i].set_opacity(0.9)
                band_dots.add(Dot(pos, color=quintile_colors[qi], radius=0.06, fill_opacity=0.9))
                all_colored_dots.add(Dot(pos, color=quintile_colors[qi], radius=0.06, fill_opacity=0.9))

            quintile_bands.add(band_dots)

        # ── Color bands behind ──────────────────────────────────────────────
        band_rects = VGroup()
        for qi in range(5):
            start_idx = qi * n_per
            end_idx = min((qi + 1) * n_per, n)
            if qi == 4:
                end_idx = n

            x1 = sorted_vals[start_idx]
            x2 = sorted_vals[end_idx - 1]

            rect = Rectangle(
                width=self.num_line.n2p(x2)[0] - self.num_line.n2p(x1)[0],
                height=1.3,
                fill_color=quintile_colors[qi],
                fill_opacity=0.15,
                stroke_width=0,
            )
            rect.move_to(self.num_line.n2p((x1 + x2) / 2) + UP * 0.5)
            band_rects.add(rect)

        # ── Animate band fill then dots changing color ──────────────────────
        self.play(
            LaggedStart(
                *[FadeIn(r) for r in band_rects],
                lag_ratio=0.3,
                run_time=2.0,
            ),
        )
        self.wait(0.5)

        # Color dots
        self.play(
            *[d.animate.set_color(quintile_colors[min(i // n_per, 4)]).set_opacity(0.9)
              for i, d in enumerate(self.dots)],
            run_time=2.0,
        )
        self.wait(1.0)

        # ── Quintile labels below ───────────────────────────────────────────
        q_labels = VGroup()
        for qi in range(5):
            start_idx = qi * n_per
            end_idx = min((qi + 1) * n_per, n)
            if qi == 4:
                end_idx = n
            mid_val = (sorted_vals[start_idx] + sorted_vals[end_idx - 1]) / 2

            label = VGroup(
                make_text(quintile_names[qi], font_size=14, color=quintile_colors[qi]),
                make_text(f"{n_per} tracts", font_size=12, color=quintile_colors[qi]),
            ).arrange(DOWN, buff=0.05)
            label.next_to(self.num_line.n2p(mid_val), DOWN, buff=0.8)
            q_labels.add(label)

        self.play(
            LaggedStart(
                *[FadeIn(l) for l in q_labels],
                lag_ratio=0.2,
                run_time=1.5,
            ),
        )
        self.wait(1.5)

        self.play(FadeOut(phase_title), run_time=0.8)
        self.wait(0.3)

        self.band_rects = band_rects
        self.q_labels = q_labels

    def phase4_insight(self):
        """Key insight: data-driven classification reveals true patterns."""

        insight_title = make_text(
            "QUANTILE CLASSIFICATION",
            font_size=30, color=AMBER, weight=BOLD,
        )
        insight_title.to_edge(UP, buff=0.4)
        self.play(Write(insight_title), run_time=1.5)
        self.wait(0.5)

        takeaways = VGroup(
            make_text(
                "1. Each tier contains the same number of observations",
                font_size=20, color=WHITE,
            ),
            make_text(
                "2. Breaks are determined by the data, not intuition",
                font_size=20, color=WHITE,
            ),
            make_text(
                "3. Reveals relative position — not raw magnitude",
                font_size=20, color=WHITE,
            ),
        )
        takeaways.arrange(DOWN, buff=0.25, aligned_edge=LEFT)
        takeaways.shift(UP * 0.2)

        self.play(
            LaggedStart(
                *[FadeIn(t, shift=RIGHT * 0.3) for t in takeaways],
                lag_ratio=0.3,
                run_time=2.5,
            ),
        )
        self.wait(2.0)

        final_note = make_text(
            "From arbitrary to data-driven classification.",
            font_size=20, color=AMBER,
        )
        final_note.next_to(takeaways, DOWN, buff=0.5)
        self.play(FadeIn(final_note), run_time=1.5)
        self.wait(2.0)

        self.play(
            FadeOut(Group(*self.mobjects)),
            run_time=1.0,
        )
        self.wait(0.3)
