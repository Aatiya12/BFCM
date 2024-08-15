WITH windows AS (
    SELECT * FROM (
                      VALUES
                          {parameter_block_1}
                  ) AS t(m_window)
),
     full_mps_ingress AS (SELECT
                              CASE
                                  WHEN sms.called LIKE '%:%' THEN split(sms.called, ':')[1]
                                  WHEN bitwise_and(sms.flags , 512) != 0 THEN 'MMS'
                                  ELSE 'SMS'
                                  END AS channel_name
                               ,CASE
                                    WHEN sms.called LIKE '%:%'
                                        THEN 'Channel Number'
                                    WHEN bitwise_and(sms.flags,2048) != 0
        THEN 'Alpha Sender'
                                    WHEN bitwise_and(sms.flags,4096) != 0
        THEN 'Short Code'
                                    WHEN from_type='LC' AND from_category='TF'
                                        THEN 'Toll Free'
                                    ELSE 'Long Code'
             END as phone_number_type
                               ,'ingress' AS ingress_egress
                               ,w.m_window AS m_window
                               ,sms.provider_id as provider
                               ,DATE_TRUNC('DAY',sms.date_created) AS date_day
                               ,date_add('second',floor((hour(sms.date_created)*3600+minute(sms.date_created)*60+second(sms.date_created))/w.m_window)*w.m_window,DATE_TRUNC('DAY',sms.date_created)) as w_minute
                               ,CASE WHEN bitwise_and(sms.flags,1) < 1
                                         THEN 'outbound' ELSE 'inbound' END AS in_out
                               ,COALESCE(SUM(sms.num_segments), 0) AS segments
                               ,COUNT(*) AS messages
                               ,SUM(CASE WHEN sms.messaging_app_sid IS NULL THEN 0 ELSE sms.num_segments END) AS messaging_service_segments
                               ,SUM(CASE WHEN sms.messaging_app_sid IS NULL THEN 0 ELSE 1 END) AS messaging_service_messages
                               ,SUM(CASE WHEN sms.account_sid IN ({read_csv}) THEN sms.num_segments ELSE 0 END) AS throughput_segments
                          FROM
                              public.rawpii_sms_kafka sms
                                  CROSS JOIN windows w
                          WHERE
    {parameter_block_2}
    AND sms.to_cc IN ('US','CA')

GROUP BY
    1,2,3,4,5,6,7,8
    ),
    fulll AS (
SELECT
    channel_name
        ,phone_number_type
        ,date_day
        ,ingress_egress
        ,m_window
        ,w_minute
        ,in_out
        ,provider
        ,CAST(segments AS real)/CAST(m_window AS real) AS tps
        ,CAST(messages AS real)/CAST(m_window AS real) AS mps
        ,CAST(messaging_service_segments AS real)/CAST(m_window AS real) AS messaging_service_tps
        ,CAST(messaging_service_messages AS real)/CAST(m_window AS real) AS messaging_service_mps
FROM
    full_mps_ingress
    ),
    allchannel AS (
SELECT
    'ALL' AS channel_name
        ,phone_number_type
        ,date_day
        ,ingress_egress
        ,m_window
        ,w_minute
        ,in_out
        ,provider
        ,COALESCE(SUM(tps), 0) AS tps
        ,COALESCE(SUM(mps), 0) AS mps
        ,COALESCE(SUM(messaging_service_tps), 0) AS messaging_service_tps
        ,COALESCE(SUM(messaging_service_mps), 0) AS messaging_service_mps
FROM
    fulll
GROUP BY
    1, 2, 3, 4, 5, 6, 7,8
    ),
    allnumber AS (
SELECT
    channel_name
        ,'ALL' AS phone_number_type
        ,date_day
        ,ingress_egress
        ,m_window
        ,w_minute
        ,in_out
        ,provider
        ,COALESCE(SUM(tps), 0) AS tps
        ,COALESCE(SUM(mps), 0) AS mps
        ,COALESCE(SUM(messaging_service_tps), 0) AS messaging_service_tps
        ,COALESCE(SUM(messaging_service_mps), 0) AS messaging_service_mps
FROM
    fulll
GROUP BY
    1, 2, 3, 4, 5, 6, 7,8
    ),
    allnumberchannel AS (
SELECT
    'ALL' AS channel_name
        ,'ALL' AS phone_number_type
        ,date_day
        ,ingress_egress
        ,m_window
        ,w_minute
        ,in_out
        ,provider
        ,COALESCE(SUM(tps), 0) AS tps
        ,COALESCE(SUM(mps), 0) AS mps
        ,COALESCE(SUM(messaging_service_tps), 0) AS messaging_service_tps
        ,COALESCE(SUM(messaging_service_mps), 0) AS messaging_service_mps
FROM
    fulll
GROUP BY
    1, 2, 3, 4, 5, 6, 7,8
    ),
    allchannelprov AS (
SELECT
    'ALL' AS channel_name
        ,phone_number_type
        ,date_day
        ,ingress_egress
        ,m_window
        ,w_minute
        ,in_out
        ,-99 AS provider
        ,COALESCE(SUM(tps), 0) AS tps
        ,COALESCE(SUM(mps), 0) AS mps
        ,COALESCE(SUM(messaging_service_tps), 0) AS messaging_service_tps
        ,COALESCE(SUM(messaging_service_mps), 0) AS messaging_service_mps
FROM
    fulll
GROUP BY
    1, 2, 3, 4, 5, 6, 7,8
    ),
    allnumberprov AS (
SELECT
    channel_name
        ,'ALL' AS phone_number_type
        ,date_day
        ,ingress_egress
        ,m_window
        ,w_minute
        ,in_out
        ,-99 AS provider
        ,COALESCE(SUM(tps), 0) AS tps
        ,COALESCE(SUM(mps), 0) AS mps
        ,COALESCE(SUM(messaging_service_tps), 0) AS messaging_service_tps
        ,COALESCE(SUM(messaging_service_mps), 0) AS messaging_service_mps
FROM
    fulll
GROUP BY
    1, 2, 3, 4, 5, 6, 7,8
    ),
    allprov AS (
SELECT
    channel_name
        ,phone_number_type
        ,date_day
        ,ingress_egress
        ,m_window
        ,w_minute
        ,in_out
        ,-99 AS provider
        ,COALESCE(SUM(tps), 0) AS tps
        ,COALESCE(SUM(mps), 0) AS mps
        ,COALESCE(SUM(messaging_service_tps), 0) AS messaging_service_tps
        ,COALESCE(SUM(messaging_service_mps), 0) AS messaging_service_mps
FROM
    fulll
GROUP BY
    1, 2, 3, 4, 5, 6, 7,8
    ),
    allall AS (
SELECT
    'ALL' AS channel_name
        ,'ALL' AS phone_number_type
        ,date_day
        ,ingress_egress
        ,m_window
        ,w_minute
        ,in_out
        ,-99 AS provider
        ,COALESCE(SUM(tps), 0) AS tps
        ,COALESCE(SUM(mps), 0) AS mps
        ,COALESCE(SUM(messaging_service_tps), 0) AS messaging_service_tps
        ,COALESCE(SUM(messaging_service_mps), 0) AS messaging_service_mps
FROM
    fulll
GROUP BY
    1, 2, 3, 4, 5, 6, 7,8
    )

SELECT
    channel_name
     ,phone_number_type
     ,provider
     ,ingress_egress
     ,date_day AS "date"
     ,m_window
     ,in_out
     ,MAX(tps) AS peak_tps
     ,MAX(mps) AS peak_mps
     ,MAX(messaging_service_tps) AS messaging_service_peak_tps
     ,MAX(messaging_service_mps) AS messaging_service_peak_mps
FROM
    ( SELECT * FROM fulll
      UNION ALL
      SELECT * FROM allchannel
      UNION ALL
      SELECT * FROM allnumber
      UNION ALL
      SELECT * FROM allnumberchannel
      UNION ALL
      SELECT * FROM allchannelprov
      UNION ALL
      SELECT * FROM allnumberprov
      UNION ALL
      SELECT * FROM allprov
      UNION ALL
      SELECT * FROM allall)
GROUP BY
    1,2,3,4,5,6,7
