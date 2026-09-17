import math


LEARNING_RATE = 0.001


def _finite(value, name):
    if not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return float(value)


def train(learning_rate=LEARNING_RATE, steps=100):
    learning_rate = _finite(learning_rate, "learning_rate")
    if not isinstance(steps, int) or isinstance(steps, bool) or steps < 0:
        raise ValueError("steps must be a non-negative integer")

    weight = 0.0
    xs = (-1.0, -0.5, 0.5, 1.0)
    for _ in range(steps):
        gradient = sum(2.0 * (weight * x - 2.0 * x) * x for x in xs) / len(xs)
        weight -= learning_rate * gradient
        weight = _finite(weight, "weight")
    return weight


def evaluate(weight):
    weight = _finite(weight, "weight")
    xs = (-0.75, -0.25, 0.25, 0.75)
    loss = sum((weight * x - 2.0 * x) ** 2 for x in xs) / len(xs)
    return _finite(loss, "loss")
