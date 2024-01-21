-- update record on transactions table
UPDATE transactions SET amount = amount + 100 where transaction_id = '<id>';

-- for debezium to capture before data, create a replica of transactions table
ALTER TABLE transactions REPLICA IDENTITY FULL;

-- add 2 more columns for tracking who modify and when they modify the record
ALTER TABLE transactions ADD COLUMN modified_by text;
ALTER TABLE transactions ADD COLUMN modified_at timestamp;

-- create a function to return current_user and timestamp
CREATE OR REPLACE FUNCTION record_change_user()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS
$$
BEGIN
NEW.modified_by := current_user;
NEW.modified_at := CURRENT_TIMESTAMP;
RETURN NEW;
END;
$$;

-- create a trigger to get user who change the data using record_change_user() function
CREATE TRIGGER trigger_record_user_update
BEFORE UPDATE ON transactions
FOR EACH ROW EXECUTE FUNCTION record_change_user(); 

-- drop current trigger on transactions table to create a new one
DROP TRIGGER trigger_record_user_update on transactions;

-- create new columns to save changes
ALTER TABLE transactions ADD COLUMN change_info jsonb;

-- create new function for capture change on amount columns only
CREATE OR REPLACE FUNCTION record_changed_column()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS
$$
DECLARE
change_details JSONB;
BEGIN
change_details := '{}'::JSONB; -- define an empty json object
-- capture change in amount
IF NEW.amount IS DISTINCT FROM OLD.amount THEN
change_details := jsonb_insert(change_details, '{amount}', jsonb_build_object('old', OLD.amount, 'new', NEW.amount));
END IF;
-- adding user and timestamp
change_details = change_details || jsonb_build_object('modified_by', current_user, 'modified_at', now());
--update change_info column
NEW.change_info := change_details;
RETURN NEW;
END;
$$;

-- create trigger
CREATE TRIGGER trigger_record_user_update
BEFORE UPDATE ON transactions
FOR EACH ROW EXECUTE FUNCTION record_changed_column();