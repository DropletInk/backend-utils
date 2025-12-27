export type PollingOptions = {
    /** Interval between polling attempts in milliseconds */
    interval: number;
    /** Maximum time to poll before timing out in milliseconds */
    timeout: number;
    backoffFactor?: number;
    retryLimit?: number;
};

export type PollingParams<T> = {
    fn: () => Promise<T>;
    /** Optional condition to check if polling should stop (if not provided, stops on first successful result) */
    stopCondition?: (result: T) => boolean;
    options: PollingOptions;
};

export type PollingResult<T> =
    | { result: T | null; attempts: number }
    | { error: Error; attempts: number };

/**
 * Polls an async function until a condition is met or timeout/retry limits are reached
 *
 * @param {PollingParams<T>} params - The polling parameters
 * @returns A promise that resolves to either a successful result or an error with attempt count
 *
 * @example
 * ```typescript
 * // Poll until a condition is met
 * const result = await poll({
 *   fn: async () => await checkStatus(),
 *   stopCondition: (status) => status === 'ready',
 *   options: { interval: 1000, timeout: 30000 }
 * });
 *
 * // Simple polling (stops on first successful result)
 * const result = await poll({
 *   fn: async () => await fetchData(),
 *   options: { interval: 500, timeout: 10000, retryLimit: 5 }
 * });
 * ```
 */
export async function poll<T>({
    fn,
    stopCondition,
    options,
}: PollingParams<T>): Promise<PollingResult<T>> {
    const {
        interval,
        timeout,
        backoffFactor = 1,
        retryLimit = 1,
    } = options;

    const retryOnError = stopCondition === undefined;
    const startTime = Date.now();
    let attempts = 0;
    let currentInterval = interval;
    let lastResult: T | null = null;
    let lastError: Error | null = null;

    while (attempts < retryLimit) {
        if (Date.now() - startTime >= timeout) {
            if (lastError && retryOnError) return { error: lastError, attempts };
            return { result: lastResult, attempts };
        }

        attempts++;

        try {
            const result = await fn();
            lastResult = result;
            lastError = null;

            if (stopCondition) {
                if (stopCondition(result)) return { result, attempts };
            } else {
                return { result, attempts };
            }
        } catch (error) {
            lastError = error instanceof Error ? error : new Error(String(error));
            if (!retryOnError) return { error: lastError, attempts };
        }

        if (Date.now() - startTime >= timeout) {
            if (lastError && retryOnError) return { error: lastError, attempts };
            return { result: lastResult, attempts };
        }

        if (attempts < retryLimit) {
            await new Promise(resolve => setTimeout(resolve, currentInterval));
            currentInterval = Math.floor(currentInterval * backoffFactor);
        }
    }

    if (lastError && retryOnError) {
        return { error: lastError, attempts };
    }
    return { result: lastResult, attempts };
}

export const PollingPresets = {
    quick: (timeout: number = 5000): PollingOptions => ({
        interval: 100,
        timeout,
        backoffFactor: 1,
        retryLimit: 50,
    }),

    standard: (timeout: number = 30000): PollingOptions => ({
        interval: 1000,
        timeout,
        backoffFactor: 1,
        retryLimit: 30,
    }),

    exponentialBackoff: (timeout: number = 60000): PollingOptions => ({
        interval: 500,
        timeout,
        backoffFactor: 1.5,
        retryLimit: 20,
    }),

    patient: (timeout: number = 300000): PollingOptions => ({
        interval: 5000,
        timeout,
        backoffFactor: 1,
        retryLimit: 60,
    }),
};


interface AnyFunction {
    (...args: any[]): any;
}

type MemoizeParams<F extends AnyFunction> = {
    fn: F;
    ttl: number;
};

/**
* This function memoizes the result of a given function for a specified TTL (time-to-live).
* @param {MemoizeParams<F>} params - An object containing the function to memoize and the TTL in milliseconds.
* @returns A new function that returns the cached result if called within the TTL, otherwise calls the original function and updates the cache.
*
* @example
* ```typescript
*  const memoizedFn = memoizeWithTTL({
*      fn: () => fetchDataFromAPI(),
*      ttl: 60000, // Cache result for 60 seconds
*  });
*  const result1 = memoizedFn(); // Calls fetchDataFromAPI()
*  const result2 = memoizedFn(); // Returns cached result if within 60 seconds
*  ```
* @example
* ```typescript
*  // Memoizing a function with no expiration
*  const memoizedSum = memoizeWithTTL({
*    fn: (a: number, b: number) => a + b,
*    ttl: Infinity, // Cache result indefinitely
*  });
*  const result1 = memoizedSum(2, 3); // Computes 5
*  const result2 = memoizedSum(2, 3); // Returns cached 5
*  ```
*  */
export function memoizeWithTTL<F extends AnyFunction>({
    fn,
    ttl,
}: MemoizeParams<F>): F {
    let cache: { value: ReturnType<F>; expiry: number } | null = null;
    return (function(...args: any[]) {
        const now = Date.now();
        if (cache && now < cache.expiry) {
            return cache.value;
        }
        const result = fn(...args);
        cache = { value: result, expiry: now + ttl };
        return result;
    }) as F;
};

export function memoize<F extends AnyFunction>(params: Omit<MemoizeParams<F>, 'ttl'>): F {
    return memoizeWithTTL<F>({ ...params, ttl: Infinity });
}
