INCLUDE '../sources/events.sql';

day_events = SELECT id, amount FROM IDENTIFIER(:input_table)
  WHERE event_date = CAST(:run_date AS DATE);

LET average_amount = SELECT AVG(amount) FROM day_events;

above_average = SELECT id, amount FROM day_events WHERE amount > :average_amount;
