export interface ApiKeyRateLimitPolicy {
  maxRequests: number;
  windowMs: number;
}

export interface ApiKeyRateLimitResult {
  allowed: boolean;
  limit: number;
  remaining: number;
  resetSeconds: number;
  retryAfterSeconds?: number;
}

interface ApiKeyRateLimitWindow {
  count: number;
  resetAt: number;
}

export const defaultApiKeyRateLimitPolicy: ApiKeyRateLimitPolicy = {
  maxRequests: 300,
  windowMs: 60_000,
};

export class InMemoryApiKeyRateLimiter {
  private readonly windows = new Map<string, ApiKeyRateLimitWindow>();
  private readonly policy: ApiKeyRateLimitPolicy;

  public constructor(
    policy: ApiKeyRateLimitPolicy = defaultApiKeyRateLimitPolicy,
    private readonly now: () => number = () => Date.now(),
  ) {
    this.policy = normalizeRateLimitPolicy(policy);
  }

  public consume(subject: string): ApiKeyRateLimitResult {
    const now = this.now();
    const window = this.currentWindow(subject, now);
    const resetSeconds = secondsUntil(window.resetAt, now);

    if (window.count >= this.policy.maxRequests) {
      return {
        allowed: false,
        limit: this.policy.maxRequests,
        remaining: 0,
        resetSeconds,
        retryAfterSeconds: resetSeconds,
      };
    }

    window.count += 1;

    return {
      allowed: true,
      limit: this.policy.maxRequests,
      remaining: this.policy.maxRequests - window.count,
      resetSeconds,
    };
  }

  private currentWindow(subject: string, now: number): ApiKeyRateLimitWindow {
    const existing = this.windows.get(subject);
    if (isActiveRateLimitWindow(existing, now)) {
      return existing;
    }

    const nextWindow = {
      count: 0,
      resetAt: now + this.policy.windowMs,
    };
    this.windows.set(subject, nextWindow);
    return nextWindow;
  }
}

function normalizeRateLimitPolicy(policy: ApiKeyRateLimitPolicy): ApiKeyRateLimitPolicy {
  assertPositiveInteger(policy.maxRequests, 'API key rate limit');
  assertPositiveInteger(policy.windowMs, 'API key rate limit window');

  return {
    maxRequests: policy.maxRequests,
    windowMs: policy.windowMs,
  };
}

function assertPositiveInteger(value: number, label: string): void {
  if (!isPositiveInteger(value)) {
    throw new Error(`invalid ${label}: ${value}`);
  }
}

function isActiveRateLimitWindow(
  window: ApiKeyRateLimitWindow | undefined,
  now: number,
): window is ApiKeyRateLimitWindow {
  return window !== undefined && now < window.resetAt;
}

function isPositiveInteger(value: number): boolean {
  return [Number.isInteger(value), value > 0].every(Boolean);
}

function secondsUntil(timestamp: number, now: number): number {
  return Math.max(1, Math.ceil((timestamp - now) / 1000));
}
