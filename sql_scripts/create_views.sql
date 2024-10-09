-- View for count of results per part and shop
CREATE OR REPLACE VIEW view_results_per_part_shop AS
SELECT part_id, supplier_id, COUNT(*) AS result_count
FROM sonar_results
GROUP BY part_id, supplier_id;

-- View for count of results per country
CREATE OR REPLACE VIEW view_results_per_country AS
SELECT s.country, COUNT(*) AS result_count
FROM sonar_results sr
JOIN suppliers s ON sr.supplier_id = s._id
GROUP BY s.country;

-- View for price development per part
CREATE OR REPLACE VIEW view_price_development AS
SELECT sr.part_id, srn.date AS run_date, AVG(sr.price_norm) AS average_price
FROM sonar_results sr
JOIN sonar_runs srn ON sr.sonar_run_id = srn._id
GROUP BY sr.part_id, srn.date
ORDER BY sr.part_id, srn.date;
