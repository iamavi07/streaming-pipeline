-- Analytical queries. Each block starts with "-- name: <query_name>".
-- "View" = one reconstructed viewing session (see README).

-- name: kpis
SELECT
    (SELECT COUNT(*) FROM streaming_events)                                        AS total_events,
    (SELECT COUNT(DISTINCT user_id) FROM streaming_events)                         AS unique_viewers,
    (SELECT COUNT(*) FROM viewing_sessions)                                        AS total_sessions,
    (SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'COMPLETED') / NULLIF(COUNT(*), 0), 2)
       FROM viewing_sessions)                                                      AS completion_rate_pct,
    (SELECT ROUND(AVG(watch_seconds) / 60.0, 2) FROM viewing_sessions)             AS avg_watch_minutes;

-- name: top_content_by_views
SELECT m.content_title, m.genre, m.content_type,
       COUNT(*)                  AS views,
       COUNT(DISTINCT s.user_id) AS unique_viewers
FROM viewing_sessions s
JOIN content_metadata m USING (content_id)
GROUP BY m.content_title, m.genre, m.content_type
ORDER BY views DESC
LIMIT 10;

-- name: events_by_content
SELECT m.content_title, COUNT(*) AS total_events
FROM streaming_events e
JOIN content_metadata m USING (content_id)
GROUP BY m.content_title
ORDER BY total_events DESC;

-- name: daily_active_viewers
SELECT event_date, COUNT(DISTINCT user_id) AS active_viewers
FROM streaming_events
GROUP BY event_date
ORDER BY event_date;

-- name: avg_viewing_duration
SELECT
    ROUND(AVG(watch_seconds) / 60.0, 2)                                         AS avg_minutes_all_sessions,
    ROUND(AVG(watch_seconds) FILTER (WHERE status <> 'INCOMPLETE') / 60.0, 2)   AS avg_minutes_closed_sessions
FROM viewing_sessions;

-- name: activity_by_region
SELECT e.region,
       COUNT(*)                  AS total_events,
       COUNT(DISTINCT e.user_id) AS unique_viewers,
       (SELECT ROUND(SUM(s.watch_seconds) / 3600.0, 1)
          FROM viewing_sessions s WHERE s.region = e.region) AS watch_hours
FROM streaming_events e
GROUP BY e.region
ORDER BY total_events DESC;

-- name: completion_rate
SELECT status,
       COUNT(*) AS sessions,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_sessions
FROM viewing_sessions
GROUP BY status
ORDER BY sessions DESC;

-- name: popular_hours
SELECT event_hour,
       COUNT(*)                                     AS total_events,
       COUNT(*) FILTER (WHERE event_type = 'PLAY')  AS play_events
FROM streaming_events
GROUP BY event_hour
ORDER BY event_hour;

-- name: device_distribution
SELECT device_type, COUNT(*) AS total_events,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_events
FROM streaming_events
GROUP BY device_type
ORDER BY total_events DESC;

-- name: popular_genres
SELECT m.genre, COUNT(DISTINCT s.session_id) AS views, COUNT(DISTINCT s.user_id) AS unique_viewers
FROM viewing_sessions s
JOIN content_metadata m USING (content_id)
GROUP BY m.genre
ORDER BY views DESC;

-- name: viewing_trend
SELECT event_date,
       SUM(total_events)                         AS total_events,
       SUM(sessions)                             AS sessions,
       ROUND(SUM(total_watch_seconds) / 3600.0, 1) AS watch_hours
FROM viewing_analytics
GROUP BY event_date
ORDER BY event_date;

-- name: dq_latest_run
SELECT check_name, records_failed, records_total,
       ROUND(100.0 * records_failed / NULLIF(records_total, 0), 2) AS failed_pct,
       run_timestamp
FROM data_quality_logs
WHERE run_id = (SELECT run_id FROM data_quality_logs ORDER BY run_timestamp DESC, log_id DESC LIMIT 1)
ORDER BY log_id;

-- name: rejection_reasons_latest_run
SELECT rejection_reasons, COUNT(*) AS rows_rejected
FROM rejected_events
GROUP BY rejection_reasons
ORDER BY rows_rejected DESC;
