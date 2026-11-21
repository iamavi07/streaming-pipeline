-- Rebuilds the daily per-content aggregate table from the base tables
TRUNCATE viewing_analytics;

INSERT INTO viewing_analytics
    (event_date, content_id, total_events, play_events, unique_viewers,
     sessions, completed_sessions, total_watch_seconds)
WITH ev AS (
    SELECT event_date, content_id,
           COUNT(*)                                   AS total_events,
           COUNT(*) FILTER (WHERE event_type = 'PLAY') AS play_events,
           COUNT(DISTINCT user_id)                    AS unique_viewers
    FROM streaming_events
    GROUP BY event_date, content_id
),
ss AS (
    SELECT start_ts::date AS event_date, content_id,
           COUNT(*)                                          AS sessions,
           COUNT(*) FILTER (WHERE status = 'COMPLETED')      AS completed_sessions,
           COALESCE(SUM(watch_seconds), 0)                   AS total_watch_seconds
    FROM viewing_sessions
    GROUP BY start_ts::date, content_id
)
SELECT ev.event_date, ev.content_id, ev.total_events, ev.play_events, ev.unique_viewers,
       COALESCE(ss.sessions, 0), COALESCE(ss.completed_sessions, 0), COALESCE(ss.total_watch_seconds, 0)
FROM ev
LEFT JOIN ss ON ss.event_date = ev.event_date AND ss.content_id = ev.content_id;
