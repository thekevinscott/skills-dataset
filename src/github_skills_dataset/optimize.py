"""Prompt optimization for skill classification using karat + DSPy."""

import json
import dspy
from karat import OpenAILM

from .filter.config import CONTENT_MAX_BYTES

OLLAMA_URL = "http://tower.tail790bbc.ts.net:11434/v1"


class ClassifySkill(dspy.Signature):
    """Classify whether a markdown file is a valid Claude Code skill.

    A valid skill has YAML frontmatter and markdown content that extends
    Claude's capabilities (instructions, workflows, knowledge, or commands).

    Reject files that are blog posts, READMEs, project docs, funding pitches,
    or configuration templates for non-Claude tools.
    """
    content: str = dspy.InputField(desc="Full text of the SKILL.md file")
    is_skill: bool = dspy.OutputField(desc="True if valid Claude Code skill, False otherwise")
    reason: str = dspy.OutputField(desc="One sentence explanation")


def load_examples(fixtures_path: str) -> list[dspy.Example]:
    """Load labeled examples from the fixtures JSON."""
    with open(fixtures_path) as f:
        fixtures = json.load(f)

    examples = []
    for fix in fixtures:
        content = fix["content"][:CONTENT_MAX_BYTES]
        ex = dspy.Example(
            content=content,
            is_skill=fix["is_skill"],
            reason=fix.get("reason", ""),
        ).with_inputs("content")
        examples.append(ex)
    return examples


def accuracy_metric(example, pred, trace=None):
    """Score: 1.0 if is_skill matches, 0.0 otherwise."""
    return float(bool(pred.is_skill) == bool(example.is_skill))


def optimize(model: str = "qwen2.5:32b", fixtures_path: str = "/tmp/e2e_fixtures_balanced.json",
             output_path: str = "data/optimized_classify_skill.json"):
    """Run DSPy optimization for the skill classifier."""
    lm = OpenAILM(model=model, base_url=OLLAMA_URL)
    dspy.configure(lm=lm)

    examples = load_examples(fixtures_path)

    # Stratified split: 20/20 train, 20/20 val
    valid = [e for e in examples if e.is_skill]
    rejected = [e for e in examples if not e.is_skill]
    train = valid[:20] + rejected[:20]
    val = valid[20:] + rejected[20:]

    print(f"Model: {model}")
    print(f"Train: {len(train)} ({sum(1 for e in train if e.is_skill)} valid, {sum(1 for e in train if not e.is_skill)} rejected)")
    print(f"Val: {len(val)} ({sum(1 for e in val if e.is_skill)} valid, {sum(1 for e in val if not e.is_skill)} rejected)")

    # Baseline
    classify = dspy.Predict(ClassifySkill)
    baseline_result = dspy.evaluate.Evaluate(
        devset=val, metric=accuracy_metric, num_threads=1, display_progress=True
    )(classify)
    print(f"\nBaseline accuracy: {baseline_result.score:.1f}%")

    # Optimize
    optimizer = dspy.MIPROv2(metric=accuracy_metric, auto="medium")
    optimized = optimizer.compile(
        classify,
        trainset=train,
        valset=val,
    )

    optimized_result = dspy.evaluate.Evaluate(
        devset=val, metric=accuracy_metric, num_threads=1, display_progress=True
    )(optimized)
    print(f"Optimized accuracy: {optimized_result.score:.1f}%")

    optimized.save(output_path)
    print(f"Saved to {output_path}")

    return optimized


if __name__ == "__main__":
    optimize()
