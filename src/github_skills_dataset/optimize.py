"""Prompt optimization for skill classification using karat + DSPy."""

import json
import dspy
from karat import OpenAILM

OLLAMA_URL = "http://tower.tail790bbc.ts.net:11434/v1"

# Truncate content aggressively -- frontmatter + first section is enough
# for classification, and keeps DSPy's few-shot prompts within context
OPTIMIZE_MAX_BYTES = 1000


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
        content = fix["content"][:OPTIMIZE_MAX_BYTES]
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


def optimize(model: str = "qwen2.5:14b",
             train_path: str = "/tmp/e2e_fixtures_train.json",
             val_path: str = "/tmp/e2e_fixtures_val.json",
             output_path: str = "data/optimized_classify_skill.json"):
    """Run DSPy optimization for the skill classifier."""
    lm = OpenAILM(model=model, base_url=OLLAMA_URL)
    dspy.configure(lm=lm, max_errors=500)

    train = load_examples(train_path)
    val = load_examples(val_path)

    print(f"Model: {model}")
    print(f"Train: {len(train)} ({sum(1 for e in train if e.is_skill)} valid, {sum(1 for e in train if not e.is_skill)} rejected)")
    print(f"Val: {len(val)} ({sum(1 for e in val if e.is_skill)} valid, {sum(1 for e in val if not e.is_skill)} rejected)")
    print(f"Content truncation: {OPTIMIZE_MAX_BYTES} bytes")

    # Warm up the model (Ollama needs ~8s to load on first request)
    print("Warming up model...")
    warmup = dspy.Predict("question -> answer")
    warmup(question="Say hello")
    print("Model loaded.\n")

    # Baseline
    classify = dspy.Predict(ClassifySkill)
    baseline_result = dspy.evaluate.Evaluate(
        devset=val, metric=accuracy_metric, num_threads=1, display_progress=True
    )(classify)
    print(f"\nBaseline accuracy: {baseline_result.score:.1f}%")

    # Optimize with BootstrapFewShot first (fast, robust to timeouts)
    print("\n--- BootstrapFewShot ---")
    bootstrap = dspy.BootstrapFewShot(metric=accuracy_metric, max_bootstrapped_demos=3, max_labeled_demos=3)
    bootstrapped = bootstrap.compile(classify, trainset=train)

    bootstrap_result = dspy.evaluate.Evaluate(
        devset=val, metric=accuracy_metric, num_threads=1, display_progress=True
    )(bootstrapped)
    print(f"BootstrapFewShot accuracy: {bootstrap_result.score:.1f}%")

    # Then try MIPROv2 for instruction optimization
    print("\n--- MIPROv2 ---")
    optimizer = dspy.MIPROv2(metric=accuracy_metric, auto="light")
    optimized = optimizer.compile(
        classify,
        trainset=train,
        valset=val,
    )

    optimized_result = dspy.evaluate.Evaluate(
        devset=val, metric=accuracy_metric, num_threads=1, display_progress=True
    )(optimized)
    print(f"MIPROv2 accuracy: {optimized_result.score:.1f}%")

    # Save the better one
    if optimized_result.score >= bootstrap_result.score:
        optimized.save(output_path)
        print(f"\nSaved MIPROv2 ({optimized_result.score:.1f}%) to {output_path}")
    else:
        bootstrapped.save(output_path)
        print(f"\nSaved BootstrapFewShot ({bootstrap_result.score:.1f}%) to {output_path}")

    return optimized


if __name__ == "__main__":
    optimize()
