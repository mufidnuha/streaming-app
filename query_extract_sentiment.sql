SELECT review_id, content 
FROM reviews
WHERE DATE_PART('month', reviews.date) = DATE_PART('month', (SELECT current_date - INTERVAL '2 month' as previous_date))
		AND DATE_PART('year', reviews.date) = DATE_PART('year', (SELECT current_date - INTERVAL '2 month' as previous_date))