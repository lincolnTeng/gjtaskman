// index.js - worker entry that re-exports both Durable Object classes
// Ensure wrangler.toml main = "index.js"

import { GjTaskman } from './gjtaskman.js';
import { HjTaskman } from './hjtaskman.js';

export { GjTaskman, HjTaskman };

export default {
  async fetch(request, env) {
    return new Response('gj-hj taskman worker active', { status: 200 });
  }
};
