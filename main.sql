SET SERVEROUTPUT ON;
BEGIN       
COMMON.LOG('Create run_New() without triggers');
END;
SELECT * FROM DBP_MESSAGE_LOG WHERE Student_ID = USER ORDER BY MSG_DATE desc;

--------------------------------------------------------
---- #Create the tables# ----
DROP TABLE local_rm16;
CREATE TABLE local_rm16 AS
    SELECT * FROM v_nem_rm16
    WHERE ROWNUM < 1;
SELECT * FROM local_rm16;

DROP TABLE dbp_parameter;
CREATE table dbp_parameter AS 
    SELECT * FROM DBP_ADMIN.dbp_parameter;
SELECT * FROM dbp_parameter;

DROP TABLE run_table;    
CREATE TABLE run_table(
    RUN_ID NUMBER, 
    RUN_START DATE,
    RUN_END DATE,
    OUTCOME VARCHAR2(15), --Assume that there are three values 'RUNNING', 'FINISHED' or NUll 
    REMARKS VARCHAR2(255)
);
SELECT * FROM run_table;
--------------------------------------------------------
---- #Data Analyst# ----
SELECT COUNT(*) FROM v_nem_rm16; --67968 = 1416 * 48 
SELECT DISTINCT TNI, LR, FRMP from nem_rm16; -- 177 combinations 
-- =>Every combination has 8 days

SELECT Day, COUNT(*) FROM nem_RM16 GROUP BY Day ORDER BY Day;
-- Only 111 records for 6/Mar, but 244 for 7/Mar

SELECT Day FROM nem_rm16 WHERE 
    TNI = 'VWO2'
    AND LR = 'EASTENGY'
    AND FRMP = 'BORAL'
    ORDER BY Day;
--------------------------------------------------------
---- #Functions and Procedures# ----

CREATE OR REPLACE FUNCTION is_Holiday (p_day IN DATE) 
RETURN BOOLEAN
    IS
---- Check if a date is holiday, i.e. in the Holiday table ----
    no_rows NUMBER;
BEGIN 
    SELECT COUNT(*) INTO no_rows
    FROM DBP_ADMIN.dbp_holiday
    WHERE holiday_date = p_day;
     
    RETURN no_rows> 0;
END;

CREATE OR REPLACE FUNCTION is_Running
RETURN BOOLEAN 
    IS
---- Check if the there is an active instance ----
    no_rows NUMBER;
    v_outcome run_table.outcome%TYPE;        
BEGIN
    SELECT count(*) INTO no_rows FROM run_table; 
    SELECT outcome INTO v_outcome 
        FROM run_table
        WHERE ROWID IN (SELECT MAX(ROWID) FROM run_table);
    
    IF no_rows = 0
    THEN
    RETURN FALSE;
    ELSE
    RETURN v_outcome = 'RUNNING';
    END IF;
END;

CREATE OR REPLACE PROCEDURE run_New(p_remark IN run_table.remarks%TYPE)
    IS
    
    running_interference EXCEPTION;
    v_max_id             run_table.run_id%TYPE;
    v_start_date         local_rm16.day%TYPE;
    v_end_date           local_rm16.day%TYPE;
    
BEGIN
    SELECT count(*) INTO v_max_id FROM run_table;
    v_start_date := TRUNC(sysdate);
    v_end_date := v_start_date + 13;
    
    IF is_Running()
    THEN
        RAISE running_interference;
    ELSE
    INSERT INTO run_table VALUES (v_max_id + 1, v_start_date, v_end_date, 'RUNNING', p_remark);
        -- considering adding trigger into it
    END IF;
    
    EXCEPTION
        WHEN running_interference THEN
            dbms_output.put_line('There is an instance running');
END;

CREATE OR REPLACE PROCEDURE run_New
    IS
    
    running_interference EXCEPTION;
    v_max_id             run_table.run_id%TYPE;
    v_start_date         local_rm16.day%TYPE;
    v_end_date           local_rm16.day%TYPE;
    
BEGIN
    SELECT count(*) INTO v_max_id FROM run_table;
    v_start_date := TRUNC(sysdate);
    v_end_date := v_start_date + 13;
    
    IF is_Running()
    THEN
        RAISE running_interference;
    ELSE
    INSERT INTO run_table VALUES (v_max_id + 1, v_start_date, v_end_date, 'RUNNING', '');
        -- considering adding trigger into it
    END IF;
    
    EXCEPTION
        WHEN running_interference THEN
            dbms_output.put_line('There is an instance running');
END;


--------------------------------------------------------
---- Testing lines -----
INSERT INTO run_table(outcome) VALUES('RUNNING'); 
EXEC run_new(' newest');
SELECT * FROM run_table;