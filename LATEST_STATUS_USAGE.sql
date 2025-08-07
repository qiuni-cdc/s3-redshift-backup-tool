
-- =============================================================================
-- LATEST PARCEL STATUS VIEWS - USAGE EXAMPLES
-- =============================================================================

-- 1. GET LATEST STATUS FOR A SPECIFIC PARCEL
SELECT * 
FROM public.settlement_latest_delivery_status 
WHERE ant_parcel_no = 'BAUNI000300014750782';

-- 2. GET ALL LATEST PARCELS FOR A PARTNER
SELECT ant_parcel_no, latest_status, last_status_update_at, net_price
FROM public.settlement_partner_latest_status 
WHERE partner_id = 27
ORDER BY last_status_update_at DESC;

-- 3. COUNT PARCELS BY STATUS (LATEST ONLY)
SELECT latest_status, COUNT(*) as parcel_count
FROM public.settlement_latest_delivery_status
GROUP BY latest_status
ORDER BY parcel_count DESC;

-- 4. FIND DELIVERED PARCELS IN LAST 24 HOURS
SELECT ant_parcel_no, partner_id, billing_num, net_price
FROM public.settlement_latest_delivery_status
WHERE latest_status = 'DELIVERED'
  AND last_status_update_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours';

-- 5. PARTNER PERFORMANCE SUMMARY (LATEST STATUSES ONLY)
SELECT 
    partner_id,
    COUNT(*) as total_parcels,
    COUNT(CASE WHEN latest_status = 'DELIVERED' THEN 1 END) as delivered_count,
    COUNT(CASE WHEN latest_status LIKE '%FAILED%' THEN 1 END) as failed_count,
    ROUND(
        COUNT(CASE WHEN latest_status = 'DELIVERED' THEN 1 END) * 100.0 / COUNT(*), 2
    ) as delivery_rate_percent
FROM public.settlement_latest_delivery_status
WHERE partner_id IS NOT NULL
GROUP BY partner_id
ORDER BY total_parcels DESC;

-- 6. STATUS SUMMARY DASHBOARD
SELECT * FROM public.settlement_status_summary;

-- 7. RECENT ACTIVITY (LATEST STATUS CHANGES)
SELECT ant_parcel_no, partner_id, latest_status, last_status_update_at
FROM public.settlement_latest_delivery_status
WHERE last_status_update_at >= CURRENT_DATE
ORDER BY last_status_update_at DESC
LIMIT 100;

-- =============================================================================
-- IMPORTANT NOTES:
-- 1. Always use settlement_latest_delivery_status for user-facing queries
-- 2. Never use settlement_normal_delivery_detail directly for status lookups
-- 3. Views automatically handle deduplication and show only latest status
-- 4. Queries are optimized with existing SORTKEY(create_at, billing_num)
-- =============================================================================
