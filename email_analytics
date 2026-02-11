WITH
  -- First CTE, pulling account metrics
  account_details AS (
    SELECT
      s.date,
      sp.country,
      a.send_interval,
      a.is_verified,
      a.is_unsubscribed,
      COUNT(DISTINCT a.id) AS account_cnt,
      0 AS sent_msg,
      0 AS open_msg,
      0 AS visit_msg
    FROM DA.session s
    JOIN DA.account_session accs
      ON s.ga_session_id = accs.ga_session_id
    JOIN DA.account a
      ON accs.account_id = a.id
    JOIN DA.session_params sp
      ON accs.ga_session_id = sp.ga_session_id
    GROUP BY
      s.date, sp.country, a.send_interval, a.is_verified, a.is_unsubscribed
  ),
  -- Second CTE, pulling email metrics
  email_details AS (
    SELECT
      DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS date,
      sp.country,
      a.send_interval,
      a.is_verified,
      a.is_unsubscribed,
      0 AS account_cnt,
      COUNT(es.id_message) AS sent_msg,
      COUNT(eo.id_message) AS open_msg,
      COUNT(ev.id_message) AS visit_msg
    FROM `DA.session` s
    JOIN `DA.session_params` sp
      ON s.ga_session_id = sp.ga_session_id
    JOIN `DA.account_session` accs
      ON s.ga_session_id = accs.ga_session_id
    JOIN `DA.account` a
      ON accs.account_id = a.id
    JOIN `DA.email_sent` es
      ON accs.account_id = es.id_account
    LEFT JOIN `DA.email_open` eo
      ON es.id_message = eo.id_message
    LEFT JOIN `DA.email_visit` ev
      ON es.id_message = ev.id_message
    GROUP BY date, sp.country, a.send_interval, a.is_verified, a.is_unsubscribed
  ),
  -- Third CTE, union of CTE #1 & #2
  final AS (
    SELECT *
    FROM account_details
    UNION ALL
    SELECT *
    FROM email_details
  ),
  -- Working with our union, calculating number of accounts by country and sent emails by country
  final_totals AS (
    SELECT
      *,
      SUM(account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
      SUM(sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt
    FROM final
  ),
  -- Calculating ranks by country
  country_totals AS (
    SELECT
      country,
      SUM(account_cnt) AS total_country_account_cnt,
      SUM(sent_msg) AS total_country_sent_cnt
    FROM final
    GROUP BY country
  ),
  country_ranks AS (
    SELECT
      country,
      total_country_account_cnt,
      total_country_sent_cnt,
      RANK()
        OVER (ORDER BY total_country_account_cnt DESC)
        AS rank_total_country_account_cnt,
      RANK()
        OVER (ORDER BY total_country_sent_cnt DESC)
        AS rank_total_country_sent_cnt
    FROM country_totals
  ),
  -- Joining ranks back to our dataset
  final_with_ranks AS (
    SELECT
      ft.*,
      cr.rank_total_country_account_cnt,
      cr.rank_total_country_sent_cnt
    FROM final_totals ft
    JOIN country_ranks cr
      ON ft.country = cr.country
  )
-- Filtering our dataset to only include top 10 countries either by # of accounts or by # of sent emails
SELECT *
FROM final_with_ranks
WHERE rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <= 10
