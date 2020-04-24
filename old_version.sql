SET SERVEROUTPUT ON;
BEGIN       
COMMON.LOG('Next time please tell us that the estimated hours for the assignment is 100000 hours, LMAO');
END;
SELECT * FROM DBP_MESSAGE_LOG WHERE Student_ID = USER ORDER BY MSG_DATE desc;

--------------------------------------------------------
---- #Create the tables# ----
DROP TABLE local_rm16;
CREATE TABLE local_rm16 AS SELECT * FROM v_nem_rm16 WHERE ROWNUM < 1;
SELECT * FROM local_rm16;
--
DROP TABLE dbp_parameter;
CREATE table dbp_parameter AS SELECT * FROM DBP_ADMIN.dbp_parameter;
SELECT * FROM dbp_parameter;
--
DROP TABLE run_table;    
CREATE TABLE run_table(
    RUN_ID NUMBER, 
    RUN_START DATE,
    RUN_END DATE,
    OUTCOME VARCHAR2(15), --Assume that there are three values 'RUNNING', 'FINISHED' or NUll 
    REMARKS VARCHAR2(255)
);
SELECT * FROM run_table;
--
DROP TABLE mean_table;
CREATE TABLE mean_table AS
    SELECT  DISTINCT TNI, FRMP, LR, TO_CHAR(day, 'DY') "DOTW", HH from v_nem_rm16
    ORDER BY TNI, FRMP, LR, DOTW, HH;
ALTER TABLE mean_table ADD last_date DATE DEFAULT TO_DATE('01/JAN/1000', 'DD/MON/YYYY');    
ALTER TABLE mean_table ADD total_volume NUMBER(10,5) DEFAULT 0;
ALTER TABLE mean_table ADD count NUMBER DEFAULT 0; 
ALTER TABLE mean_table ADD average NUMBER(10,5);
ALTER TABLE mean_table ADD dummy_date DATE;
SELECT * FROM mean_table;
--
DROP TABLE mean_table_holiday;
CREATE TABLE mean_table_holiday AS
    SELECT  DISTINCT TNI, FRMP, LR, HH from v_nem_rm16
    ORDER BY TNI, FRMP, LR, HH;
ALTER TABLE mean_table_holiday ADD last_date DATE DEFAULT TO_DATE('01/JAN/1000', 'DD/MON/YYYY');        
ALTER TABLE mean_table_holiday ADD total_volume NUMBER(10,5) DEFAULT 0;
ALTER TABLE mean_table_holiday ADD count NUMBER DEFAULT 0; 
ALTER TABLE mean_table_holiday ADD average NUMBER(10,5);
ALTER TABLE mean_table_holiday ADD dummy_date DATE;
SELECT * FROM mean_table_holiday;

--------------------------------------------------------
---- #Data Analyst# ----
SELECT COUNT(*) FROM v_nem_rm16; --67968 = 1416 * 48 
SELECT DISTINCT TNI, LR, FRMP from nem_rm16; -- 177 combinations 
-- =>Every combination has 8 days

SELECT Day, COUNT(*) FROM nem_RM16 GROUP BY Day ORDER BY Day;
-- Only 111 records for 6/Mar, but 244 for 7/Mar
SELECT COUNT (*) FROM v_nem_RM16 GROUP BY TNI, LR, FRMP;
SELECT Day FROM nem_rm16 WHERE 
    TNI = 'VWO2'
    AND LR = 'EASTENGY'
    AND FRMP = 'BORAL'
    ORDER BY Day;
    
SELECT TNI, LR,FRMP ,COUNT(DISTINCT DAY) FROM v_nem_rm16 GROUP BY TNI, LR, FRMP;
--------------------------------------------------------
---- #Functions and Procedures# ----
CREATE OR REPLACE FUNCTION fetch_Param(p_category U13305952.dbp_parameter.category%TYPE
                                     , p_code     U13305952.dbp_parameter.code%TYPE)
RETURN U13305952.dbp_parameter.value%TYPE
    IS
    v_value U13305952.dbp_parameter.value%TYPE;
BEGIN
    SELECT value INTO v_value 
        FROM U13305952.dbp_parameter
        WHERE category = p_category AND code = p_code;
    
    RETURN v_value;
END;
-- First Stage:run_New
CREATE OR REPLACE FUNCTION is_Running
RETURN BOOLEAN 
    IS
---- Check if the there is an active instance ----
    no_rows  run_table.run_id%TYPE;
    v_outcome run_table.outcome%TYPE;        
BEGIN
    SELECT count(*) INTO no_rows FROM run_table; 
    
    IF no_rows = 0
    THEN
        RETURN FALSE;
    ELSE
    SELECT outcome INTO v_outcome 
        FROM run_table
        WHERE ROWID = (SELECT MAX(ROWID) FROM run_table);
        RETURN v_outcome = 'RUNNING';
    END IF;
END;

CREATE OR REPLACE PROCEDURE run_New
    IS
---- Run if there is no active instance ----
    running_interference EXCEPTION;
    v_max_id             run_table.run_id%TYPE;
    v_start_date         local_rm16.day%TYPE;
    v_end_date           local_rm16.day%TYPE;
    
BEGIN
    SELECT count(*) INTO v_max_id FROM run_table;
    v_start_date := TRUNC(sysdate);
    v_end_date := v_start_date + 13;
    
    IF is_running()
    THEN
        RAISE running_interference;
    ELSE
    INSERT INTO run_table VALUES (v_max_id + 1, v_start_date, v_end_date, 'RUNNING','');
        -- considering adding trigger into it
    END IF;
    
    EXCEPTION
        WHEN running_interference THEN
            dbms_output.put_line('There is an instance running');
END;

CREATE OR REPLACE PROCEDURE finish_New
    IS
BEGIN
    UPDATE run_table
    SET
        outcome = 'FINISHED'
    WHERE 
        ROWID = (SELECT MAX(ROWID) FROM run_table);
END;

CREATE OR REPLACE PROCEDURE update_mean_table
                            (p_tni  mean_table.tni%TYPE,
                             p_frmp mean_table.frmp%TYPE,
                             p_lr   mean_table.tni%TYPE,
                             p_hh   mean_table.hh%TYPE, 
                             p_last_date  mean_table.last_date%TYPE,
                             p_DOTW mean_table.dotw%TYPE
                            )
    IS 
    -- #Update mean_table 
    no_rows     mean_table.count%TYPE;   
    v_volume    v_nem_rm16.volume%TYPE; 
    v_date      v_nem_rm16.day%TYPE;      
BEGIN
    -- Count the number of rows in v_nem_rm16 that match
    SELECT COUNT(*) INTO no_rows  
        FROM v_nem_rm16 
        WHERE 
                TNI = p_tni 
            AND FRMP = p_frmp
            AND LR = p_lr 
            AND HH = p_hh
            AND day > p_last_date
            AND TO_CHAR(day, 'DY') = p_DOTW
            AND day NOT IN (SELECT * FROM dbp_holiday); -- non-holiday date
    -- Proceed only when there is something match
    IF no_rows > 0
    THEN
        SELECT sum(volume) INTO v_volume  
        FROM v_nem_rm16 
        WHERE 
                TNI = p_tni 
            AND FRMP = p_frmp
            AND LR = p_lr 
            AND HH = p_hh
            AND day > p_last_date
            AND TO_CHAR(day, 'DY') = p_DOTW
            AND day NOT IN (SELECT * FROM dbp_holiday); -- non-holiday date
        
        SELECT day INTO v_date  
        FROM v_nem_rm16 
        WHERE 
                TNI = p_tni 
            AND FRMP = p_frmp
            AND LR = p_lr 
            AND HH = p_hh
            AND day > p_last_date
            AND TO_CHAR(day, 'DY') = p_DOTW
            AND day NOT IN (SELECT * FROM dbp_holiday) -- non-holiday date
            AND ROWNUM = 1 ORDER BY day DESC; -- Get the latest date only
        
        UPDATE mean_table 
        SET 
            count = count + no_rows,
            last_date = v_date,
            total_volume = total_volume + v_volume,
            average = (total_volume + v_volume) / (count + no_rows)
            WHERE 
                    TNI = p_tni 
                AND FRMP = p_frmp
                AND LR = p_lr 
                AND HH = p_hh
                AND DOTW = TO_CHAR(p_last_date, 'DY');

        
        DBMS_OUTPUT.PUT_LINE(v_volume);
        DBMS_OUTPUT.PUT_LINE(v_date);
    
    ELSE
        dbms_output.put_line(0);
    END IF;
END;
           
CREATE OR REPLACE PROCEDURE update_mean_table_holiday
                            (p_tni  mean_table.tni%TYPE,
                             p_frmp mean_table.frmp%TYPE,
                             p_lr   mean_table.tni%TYPE,
                             p_hh   mean_table.hh%TYPE, 
                             p_last_date  mean_table.last_date%TYPE
                            )
    IS 
    -- #Update mean_table 
    no_rows     mean_table.count%TYPE;   
    v_volume    v_nem_rm16.volume%TYPE; 
    v_date      v_nem_rm16.day%TYPE;      
BEGIN
    -- Count the number of rows in v_nem_rm16 that match
    SELECT COUNT(*) INTO no_rows  
        FROM v_nem_rm16 
        WHERE 
                TNI = p_tni 
            AND FRMP = p_frmp
            AND LR = p_lr 
            AND HH = p_hh
            AND day > p_last_date
            AND day IN (SELECT * FROM dbp_holiday); -- non-holiday date
    -- Proceed only when there is something match
    IF no_rows > 0
    THEN
        SELECT sum(volume) INTO v_volume  
        FROM v_nem_rm16 
        WHERE 
                TNI = p_tni 
            AND FRMP = p_frmp
            AND LR = p_lr 
            AND HH = p_hh
            AND day > p_last_date
            AND day NOT IN (SELECT * FROM dbp_holiday); -- non-holiday date
        
        SELECT day INTO v_date  
        FROM v_nem_rm16 
        WHERE 
                TNI = p_tni 
            AND FRMP = p_frmp
            AND LR = p_lr 
            AND HH = p_hh
            AND day > p_last_date
            AND day NOT IN (SELECT * FROM dbp_holiday) -- non-holiday date
            AND ROWNUM = 1 ORDER BY day DESC; -- Get the latest date only
        
        UPDATE mean_table 
        SET 
            count = count + no_rows,
            last_date = v_date,
            total_volume = total_volume + v_volume,
            average = (total_volume + v_volume)/(count + no_rows)
            WHERE 
                    TNI = p_tni 
                AND FRMP = p_frmp
                AND LR = p_lr 
                AND HH = p_hh;

        
        DBMS_OUTPUT.PUT_LINE(v_volume);
        DBMS_OUTPUT.PUT_LINE(v_date);
    
    ELSE
        dbms_output.put_line(0);
    END IF;
END;

CREATE OR REPLACE PROCEDURE update_all_mean_table
    IS
    -- Update mean_table and mean_table_holiday 
    CURSOR c_mean IS
        SELECT * FROM mean_table;
    
    CURSOR c_mean_holiday IS
        SELECT * FROM mean_table_holiday;    

BEGIN
    FOR r_mean IN c_mean
    LOOP    
        update_mean_table(p_tni => r_mean.tni, 
                          p_frmp => r_mean.frmp, 
                          p_lr => r_mean.lr,
                          p_hh => r_mean.hh,
                          p_last_date => r_mean.last_date,
                          p_dotw => r_mean.dotw);
    END LOOP;
    
    FOR r_mean IN c_mean_holiday
    LOOP
        update_mean_table_holiday(
                          p_tni => r_mean.tni, 
                          p_frmp => r_mean.frmp, 
                          p_lr => r_mean.lr,
                          p_hh => r_mean.hh,
                          p_last_date => r_mean.last_date);
    END LOOP;                      
END;       

EXEC update_all_mean_table()
CREATE OR REPLACE FUNCTION is_Holiday(p_date DATE)
RETURN BOOLEAN
    IS
    
    v_count NUMBER;
BEGIN
    SELECT count(*) INTO v_count
    FROM dbp_holiday
    WHERE holiday_date = TRUNC(p_date);
    
    RETURN v_count = 1;
END;

CREATE OR REPLACE Procedure predict
    IS 
    v_count INTEGER := 0;
    v_current DATE := TRUNC(sysdate);
BEGIN
    WHILE v_count < 14
    LOOP
        
        IF NOT is_Holiday(v_current)
        THEN 
            UPDATE mean_table
            SET 
                dummy_date = v_current;
            
            INSERT INTO local_rm16(tni,frmp,lr,hh, volume, day)
                SELECT tni, frmp, lr, hh, average, dummy_date
                FROM mean_table;
        ELSE
            UPDATE mean_table_holiday
            SET 
                dummy_date = v_current; 
            
            INSERT INTO local_rm16(tni,frmp,lr,hh, volume, day)
                SELECT tni, frmp, lr, hh, average, dummy_date
                FROM mean_table_holiday;
        END IF;
        v_count := v_count + 1;
        v_current := v_current + v_count;
    END LOOP;
    
END;


CREATE OR REPLACE PROCEDURE main
    IS
   
BEGIN
    run_NEW();
    update_all_mean_table()
END;

SELECT sysdate FROM dual

--------------------------------------------------------
---- Testing lines -----
INSERT INTO run_table(outcome) VALUES('RUNNING'); 
EXEC run_new(' newest');
SELECT * FROM run_table;