/** Secrets supplied to the cell at deployment or node startup. */
interface Env {
  readonly OPENAI_API_KEY: string;
}

declare namespace Cloudflare {
  interface Env {
    readonly OPENAI_API_KEY: string;
  }
}
