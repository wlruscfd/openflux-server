-- The client that passes a provider's check also learns the final document address after the
-- browser's own redirects. A share URL like /i/<key> is resolved by script in the browser and
-- gives a node (no JS) a landing page with no editor config, so the resolved /edit/ address rides
-- along with the jar.
ALTER TABLE key_cookies ADD COLUMN IF NOT EXISTS resolved_url text NOT NULL DEFAULT '';