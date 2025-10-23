import time, random

def now_ts() -> int:
    return int(time.time())

def tier_weights(on_duty_count: int):
    w1, w2, w3, w4 = 0.40, 0.35, 0.20, 0.05
    delta = max(0.0, min(0.6, (on_duty_count - 6) / 12.0))
    w1 -= 0.20 * delta
    w2 -= 0.10 * delta
    w3 += 0.20 * delta
    w4 += 0.10 * delta
    s = w1 + w2 + w3 + w4
    return (w1/s, w2/s, w3/s, w4/s)

def weighted_choice(rng: random.Random, weights):
    total = sum(weights)
    r = rng.random() * total
    acc = 0.0
    for i, w in enumerate(weights):
        acc += w
        if r <= acc:
            return i
    return len(weights) - 1
