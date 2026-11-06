-- The room a transport makes for itself.
--
-- A cupsonline or avito key has no document: the exit creates the rooms and the client
-- needs the list of them. Until now the panel only knew whether the list had ever arrived,
-- and it never accepted a second one, so a key whose rooms had died kept pointing at the
-- dead ones and the client had nothing to open.
--
-- The state says whether the rooms exist right now, and the error says why not, so the
-- panel can show it instead of leaving an empty document to be guessed at.
ALTER TABLE keys ADD COLUMN IF NOT EXISTS room_state text NOT NULL DEFAULT 'unknown';
ALTER TABLE keys ADD COLUMN IF NOT EXISTS room_state_at timestamptz;
ALTER TABLE keys ADD COLUMN IF NOT EXISTS room_state_error text;
ALTER TABLE keys ADD COLUMN IF NOT EXISTS room_list_at timestamptz;

CREATE INDEX IF NOT EXISTS keys_room_state_idx ON keys (room_state) WHERE transport IN ('cupsonline', 'avito');
