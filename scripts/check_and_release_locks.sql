-- Redshift Lock Checking and Release Script
-- Run this script to diagnose and release locks

-- ============================================
-- STEP 1: Check current locks
-- ============================================
SELECT
    'Current Locks' as info,
    database,
    relation,
    pid,
    granted
FROM pg_locks
ORDER BY pid, granted;

-- ============================================
-- STEP 2: Check if the lock is from your session
-- ============================================
SELECT
    'Your Session PID' as info,
    pg_backend_pid() as my_pid;

-- ============================================
-- STEP 3: Find table names for locked relations
-- ============================================
SELECT
    'Locked Tables' as info,
    n.nspname as schema_name,
    c.relname as table_name,
    l.pid,
    l.granted
FROM pg_locks l
LEFT JOIN pg_class c ON l.relation = c.oid
LEFT JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE c.relname IS NOT NULL
ORDER BY l.pid;

-- ============================================
-- STEP 4: Check for recent long-running queries
-- ============================================
-- Note: This requires access to stl_query
-- If you don't have access, skip this step
SELECT
    'Recent Queries' as info,
    query,
    SUBSTRING(querytxt, 1, 100) as query_text,
    starttime,
    endtime,
    DATEDIFF(second, starttime, COALESCE(endtime, GETDATE())) as duration_seconds,
    CASE WHEN endtime IS NULL THEN 'RUNNING' ELSE 'COMPLETED' END as status
FROM stl_query
WHERE userid = (SELECT usesysid FROM pg_user WHERE usename = current_user)
  AND starttime > CURRENT_TIMESTAMP - INTERVAL '2 hours'
ORDER BY starttime DESC
LIMIT 20;

-- ============================================
-- STEP 5: Release locks (MANUAL EXECUTION)
-- ============================================
-- IMPORTANT: Only run the commands below MANUALLY after reviewing the output above

-- Option A: Cancel a running query (safer)
-- SELECT pg_cancel_backend(<PID>);

-- Option B: Terminate a session (more aggressive)
-- SELECT pg_terminate_backend(<PID>);

-- Option C: If it's your own transaction
-- ROLLBACK;
-- COMMIT;

-- ============================================
-- Example usage:
-- Replace <PID> with the actual process ID from STEP 1
-- ============================================
-- To cancel: SELECT pg_cancel_backend(1073815845);
-- To terminate: SELECT pg_terminate_backend(1073815845);
