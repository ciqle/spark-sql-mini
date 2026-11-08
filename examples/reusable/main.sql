INCLUDE 'queries/above_average.sql';

SELECT CAST(:run_date AS DATE) AS run_date, :average_amount AS average_amount, id, amount
FROM above_average ORDER BY id;
