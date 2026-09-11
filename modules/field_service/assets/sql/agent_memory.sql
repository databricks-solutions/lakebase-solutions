-- ============================================================================
-- AI AGENT LONG-TERM MEMORY
-- Persistent conversation storage in Lakebase PostgreSQL
-- ============================================================================
--
-- Stores multi-turn conversations for the LangGraph multi-agent supervisor.
-- Enables follow-up questions, conversation resumption, and memory search.
--
-- Architecture:
--   Browser → Flask (inject history) → Model Serving (stateless) → Genie agents
--   Flask persists Q&A pairs to these tables after each turn.
--   On next turn, Flask fetches history and sends it with the new question.
--
-- Demo value: "Your OLTP database IS your AI memory layer. No separate
--   vector DB, no external memory service. All Lakebase."
--
-- Idempotent: Uses IF NOT EXISTS for clean re-runs.
-- ============================================================================

SET client_min_messages = NOTICE;

-- ── Schema ──────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS ai_memory;

COMMENT ON SCHEMA ai_memory IS
    'Long-term memory for the AI multi-agent supervisor. '
    'Stores conversations and messages in Lakebase PG, enabling multi-turn '
    'dialogue, conversation resumption, and full-text memory search.';

-- ── App Users (auto-populated on first interaction) ─────────────────────
CREATE TABLE IF NOT EXISTS ai_memory.app_users (
    user_id      VARCHAR(255) PRIMARY KEY,
    display_name VARCHAR(200),
    role         VARCHAR(20) DEFAULT 'user' CHECK (role IN ('user', 'admin', 'dispatcher', 'manager')),
    first_seen   TIMESTAMPTZ DEFAULT now(),
    last_seen    TIMESTAMPTZ DEFAULT now(),
    preferences  JSONB DEFAULT '{}'
);

COMMENT ON TABLE ai_memory.app_users IS
    'Application users extracted from Databricks OAuth tokens. Role determines '
    'conversation visibility: admin sees all, others see only their own.';

-- ── User-Region mapping (RBAC) ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ai_memory.user_region_mapping (
    user_id    VARCHAR(255) NOT NULL,
    region_id  INTEGER NOT NULL,
    PRIMARY KEY (user_id, region_id)
);

COMMENT ON TABLE ai_memory.user_region_mapping IS
    'Maps app users to the service regions they can access. '
    'Admins bypass this filter. Default: users with no mapping see all regions.';

-- ── Conversations ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ai_memory.conversations (
    conversation_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          VARCHAR(255) REFERENCES ai_memory.app_users(user_id),
    title            VARCHAR(200),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    message_count    INT NOT NULL DEFAULT 0,
    summary          TEXT,
    metadata         JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_conv_updated
    ON ai_memory.conversations (updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_conv_user
    ON ai_memory.conversations (user_id, updated_at DESC);

COMMENT ON TABLE ai_memory.conversations IS
    'Chat threads with the AI supervisor. Each conversation belongs to a user '
    '(user_id from Databricks OAuth), has a title (auto-set from first question), '
    'a message count, and an optional LLM-generated summary for context windowing.';

-- ── Messages ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ai_memory.messages (
    message_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id  UUID NOT NULL REFERENCES ai_memory.conversations(conversation_id) ON DELETE CASCADE,
    role             VARCHAR(20) NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
    content          TEXT NOT NULL,
    spaces_consulted TEXT[],
    token_estimate   INT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata         JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_msg_conv_created
    ON ai_memory.messages (conversation_id, created_at);

-- Full-text search index for memory search
CREATE INDEX IF NOT EXISTS idx_msg_content_fts
    ON ai_memory.messages USING GIN (to_tsvector('english', content));

COMMENT ON TABLE ai_memory.messages IS
    'Individual turns in a conversation. role=user for questions, '
    'role=assistant for supervisor responses. spaces_consulted tracks '
    'which Genie agents were invoked for each response.';

-- ── Auto-update trigger ─────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION ai_memory.trg_update_conversation()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE ai_memory.conversations
    SET updated_at = now(),
        message_count = (
            SELECT COUNT(*) FROM ai_memory.messages
            WHERE conversation_id = NEW.conversation_id
        )
    WHERE conversation_id = NEW.conversation_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_msg_update_conv ON ai_memory.messages;
CREATE TRIGGER trg_msg_update_conv
    AFTER INSERT ON ai_memory.messages
    FOR EACH ROW EXECUTE FUNCTION ai_memory.trg_update_conversation();

-- ── Memory search function ──────────────────────────────────────────────
CREATE OR REPLACE FUNCTION ai_memory.search_memory(
    query_text TEXT,
    max_results INT DEFAULT 10
)
RETURNS TABLE (
    conversation_id UUID,
    conversation_title VARCHAR(200),
    message_id UUID,
    role VARCHAR(20),
    content TEXT,
    created_at TIMESTAMPTZ,
    rank REAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT m.conversation_id,
           c.title,
           m.message_id,
           m.role,
           m.content,
           m.created_at,
           ts_rank(to_tsvector('english', m.content),
                   plainto_tsquery('english', query_text)) as rank
    FROM ai_memory.messages m
    JOIN ai_memory.conversations c ON c.conversation_id = m.conversation_id
    WHERE to_tsvector('english', m.content) @@ plainto_tsquery('english', query_text)
    ORDER BY rank DESC
    LIMIT max_results;
END;
$$ LANGUAGE plpgsql;

-- ── Grants ──────────────────────────────────────────────────────────────
-- Grant to every role that can already use field_service, rather than to a
-- hardcoded list of role names.
--
-- The previous version granted only to 'lakebase_app' and 'lakebase_app_perms'.
-- The app does not connect as either: 10b_setup_secrets.py sets pguser to the
-- app's service principal id, so the app's PG role is that UUID. It therefore had
-- no rights here at all, and every write failed with
--   "permission denied for schema ai_memory"
-- while the UI still reported "Saved Q&A to Lakebase ai_memory".
--
-- Deriving the grantees from field_service keeps this correct no matter what the
-- app's role is called — if a role can read the application schema, it should be
-- able to use conversation memory.
DO $$
DECLARE
    r TEXT;
BEGIN
    FOR r IN
        SELECT rolname
        FROM pg_roles
        WHERE rolname NOT LIKE 'pg\_%'
          AND rolname <> current_user
          AND has_schema_privilege(rolname, 'field_service', 'USAGE')
    LOOP
        EXECUTE format('GRANT USAGE ON SCHEMA ai_memory TO %I', r);
        EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ai_memory TO %I', r);
        EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ai_memory TO %I', r);
        EXECUTE format('GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ai_memory TO %I', r);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA ai_memory GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I', r);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA ai_memory GRANT USAGE, SELECT ON SEQUENCES TO %I', r);
        RAISE NOTICE 'ai_memory granted to %', r;
    END LOOP;
END $$;

-- ============================================================================
-- DONE
-- ============================================================================
