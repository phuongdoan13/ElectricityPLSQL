SET SERVEROUTPUT ON;
BEGIN       
COMMON.LOG('Final logic');
END;
--------------------------------------------------------

--------------------------------------------------------
---- #Procedures and Functions# ----
CREATE OR REPLACE  FUNCTION fetch_Param(p_category U13305952.dbp_parameter.category%TYPE
                                     , p_code     U13305952.dbp_parameter.code%TYPE)
RETURN U13305952.dbp_parameter.value%TYPE
    IS
    v_value U13305952.dbp_parameter.value%TYPE;
BEGIN
    SELECT value INTO v_value 
        FROM dbp_parameter
        WHERE category = p_category AND code = p_code;

    RETURN v_value;
END;

CREATE OR REPLACE PROCEDURE write_localrm16(p_tni local_rm16.tni%TYPE,
                                            p_frmp local_rm16.frmp%TYPE,
                                            p_lr local_rm16.lr%TYPE,
                                            p_hh local_rm16.hh%TYPE,
                                            p_day local_rm16.day%TYPE,
                                            p_volume local_rm16.volume%TYPE)
    IS
    -- #Write prediciton into local_rm16 --
BEGIN
    INSERT INTO local_rm16(tni, frmp, lr, hh, day, volume, statement_type, change_date) 
    VALUES (p_tni, p_frmp, p_lr, p_hh, p_day, p_volume, 'FORECAST', sysdate);
END;

CREATE OR REPLACE PROCEDURE calculate_mean(p_tni v_nem_rm16.tni%TYPE,
                                           p_frmp v_nem_rm16.frmp%TYPE,
                                           p_lr v_nem_rm16.lr%TYPE,
                                           p_hh v_nem_rm16.hh%TYPE,
                                           p_day v_nem_rm16.day%TYPE,
                                           p_dotw varchar2)
    IS
    -- #Calculate the mean volume for a specific combination --
    v_mean v_nem_rm16.volume%TYPE;
BEGIN
    SELECT AVG(volume) INTO v_mean
    FROM v_nem_rm16
    WHERE 
        tni = p_tni
    AND frmp = p_frmp
    AND lr = p_lr
    AND hh = p_hh
    AND TO_CHAR(day, 'DY') = p_dotw
    AND day NOT IN (SELECT * FROM dbp_holiday);

    write_localrm16(p_tni, p_frmp, p_lr, p_hh, p_day, v_mean);
END;  

CREATE OR REPLACE PROCEDURE calculate_holiday_mean(p_tni v_nem_rm16.tni%TYPE,
                                                   p_frmp v_nem_rm16.frmp%TYPE,
                                                   p_lr v_nem_rm16.lr%TYPE,
                                                   p_hh v_nem_rm16.hh%TYPE,
                                                   p_day v_nem_rm16.day%TYPE)
    IS
    -- #Calculate the holiday mean volume for a specific combination --
    v_mean v_nem_rm16.volume%TYPE;
    v_count_holiday NUMBER;
BEGIN
    SELECT count(*) INTO v_count_holiday
    FROM v_nem_rm16
    WHERE day IN (SELECT * FROM dbp_holiday)
    AND tni = p_tni
    AND frmp = p_frmp
    AND lr = p_lr
    AND hh = p_hh;
    
    IF (v_count_holiday > 0)
    THEN
        SELECT AVG(volume) INTO v_mean
        FROM v_nem_rm16
        WHERE 
            tni = p_tni
        AND frmp = p_frmp
        AND lr = p_lr
        AND hh = p_hh
        AND day IN (SELECT * FROM dbp_holiday);
    ELSE 
        SELECT AVG(volume) INTO v_mean
        FROM v_nem_rm16
        WHERE 
            tni = p_tni
        AND frmp = p_frmp
        AND lr = p_lr
        AND hh = p_hh
        AND TO_CHAR(day, 'DY') = 'SUN' ;
    END IF;
    
    write_localrm16(p_tni, p_frmp, p_lr, p_hh, p_day, v_mean);
END; 

CREATE OR REPLACE FUNCTION is_Holiday(p_date DATE)
RETURN BOOLEAN
    IS
    -- #Check if the input date is a holiday --
    v_count NUMBER;
BEGIN
    SELECT count(*) INTO v_count
    FROM dbp_holiday
    WHERE holiday_date = TRUNC(p_date);

    RETURN v_count = 1;
END;

CREATE OR REPLACE PROCEDURE predict
    IS
    -- #Predict for all the combinations in the next 14 days --
    v_current DATE := TRUNC(sysdate);
    v_counter NUMBER := 0;
    CURSOR c_combinations IS 
        SELECT DISTINCT tni, frmp, lr, hh FROM v_nem_rm16;
    v_mean v_nem_rm16.VOLUME % TYPE;
BEGIN
    TRUNCATE TABLE local_rm16;
    WHILE v_counter < 14
    LOOP
        v_current := v_current + v_counter;
        FOR r_combi IN c_combinations
        LOOP
            IF is_Holiday(v_current)
            THEN
                calculate_holiday_mean(p_tni => r_combi.tni,  p_frmp => r_combi.frmp, p_lr => r_combi.lr, p_hh => r_combi.hh, p_day => v_current);
            ELSE
                calculate_mean(p_tni => r_combi.tni,  p_frmp => r_combi.frmp, p_lr => r_combi.lr, p_hh => r_combi.hh, p_day => v_current, p_dotw => TO_CHAR(v_current, 'DY'));
            END IF;           
        END LOOP;
        v_counter := v_counter + 1;
    END LOOP;
END;

--
create sequence seq_runlog start with 1 maxvalue 999999999 increment by 1; 
CREATE OR REPLACE PROCEDURE RM16_forecast
    IS
    -- #Main --

    v_runTableRec run_table%ROWTYPE;
    v_runTableID NUMBER; 
    const_RunBuffer CONSTANT NUMBER := 1;
    moduleRan EXCEPTION;
BEGIN    
    BEGIN
        SELECT * INTO v_runtablerec
        FROM run_table
        WHERE 
            outcome = 'SUCCESS'
        AND run_end > (sysdate - const_RunBuffer);
        
        RAISE moduleRan; 
        -- If the code reaches this line, that means there might be a run in the last 24 hours
        
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                SELECT seq_runlog.NEXTVAL INTO v_runTableID FROM dual;
                INSERT INTO run_table(run_id, run_start, run_end, outcome, remarks)
                VALUES (v_runTableID, sysdate, NULL, NULL, 'Start Program');
    END;
    
    -- main --
    predict();
    --
    
    UPDATE run_table
    SET run_end = sysdate,
        outcome = 'SUCCESS',
        remarks = 'Run completed successfully'
    WHERE run_id = v_runTableID;
    
    EXCEPTION 
        WHEN moduleRan THEN
        DBMS_OUTPUT.put_line('Check the RUN_LOG. Module ran in the last hour, handled in main block');
END;


EXEC RM16_forecast();

set serveroutput on;
DECLARE
  Ctx               DBMS_XMLGEN.ctxHandle;
  xml               CLOB := NULL;
  temp_xml          CLOB := NULL;
  v_query_date      varchar2(25) := To_char(sysdate);
  QUERY    VARCHAR2(2000) := 'SELECT tni, sum(volume) tni_total 
                              FROM local_rm16
                              WHERE DAY = '''||v_query_date||''' GROUP BY tni';
BEGIN
dbms_output.put_line(query);
   Ctx := DBMS_XMLGEN.newContext(QUERY);
   DBMS_XMLGen.setRowsetTag( Ctx, 'ROWSETTAG' );
   DBMS_XMLGen.setRowTag( Ctx, 'ROWTAG' );
   temp_xml := DBMS_XMLGEN.getXML(Ctx);
--
        IF temp_xml IS NOT NULL THEN
            IF xml IS NOT NULL THEN
                DBMS_LOB.APPEND( xml, temp_xml );
            ELSE
                xml := temp_xml;
            END IF;
        END IF;
--
        DBMS_XMLGEN.closeContext( Ctx );
        dbms_output.put_line(xml);
end;


DROP DIRECTORY S13305952_DIR;
CREATE DIRECTORY S13305952_DIR AS '/exports/orcloz';

DECLARE 
    v_file  utl_file.file_type;
    v_date  VARCHAR2(20) := TO_CHAR(sysdate, 'DD-MON-YYYY');
BEGIN
    v_file := utl_file.fopen('S13305952_DIR', 'laurie.txt', 'W');
    utl_file.put_line(v_file, 'LOL');
    utl_file.fclose(v_file);
    EXCEPTION
        WHEN OTHERs Then
            utl_file.fclose(v_file);
END;
set serveroutput on;





DECLARE
  v_file  utl_file.file_type;
  v_date  VARCHAR2(20) := to_char(sysdate, 'DD-MON-YYYY HH24:MI:SS');
BEGIN
v_file := utl_file.fopen (‘<Student_nr>_DIR','laurie.test', 'A');
  utl_file.put_line(v_file, 'Test print at '||v_date);
  utl_file.put_line(v_file, 'Test print at '||lpad('laurie',15));
  utl_file.fclose(v_file);
END;

ALTER TABLE dbp_parameter
drop column created
--RENAME COLUMN modified_by TO modified;
SELECT * FROM dbp_parameter
INSERT INTO dbp_parameter(category, code, value, active, modified)
VALUES ('Date format', 'df2', 'DD/MON/YYYY', 'Y', sysdate);

CREATE OR REPLACE PROCEDURE write_xml
AS
    Ctx               DBMS_XMLGEN.ctxHandle;
    xml               CLOB := NULL;
    temp_xml          CLOB := NULL;
    v_query_date      varchar2(25) := TO_CHAR(sysdate + 1, fetch_Param('Date format', 'df2'));
    QUERY             VARCHAR2(2000) := 'SELECT tni, sum(volume) tni_total 
                              FROM local_rm16
                              WHERE DAY = '''||v_query_date||''' GROUP BY tni';
    my_dir            VARCHAR2(100) := fetch_Param('my_dir', 'md_final');
    filename          VARCHAR2(100) := 'U' || fetch_Param('student_id', 'id') || '_' || TO_CHAR(sysdate + 1, fetch_Param('Date format','df1')) || '.xml';
    v_file            utl_file.file_type;
BEGIN
    dbms_output.put_line(query);
    Ctx := DBMS_XMLGEN.newContext(QUERY);
    DBMS_XMLGen.setRowsetTag( Ctx, 'ROWSETTAG' );
    DBMS_XMLGen.setRowTag( Ctx, 'ROWTAG' );
    temp_xml := DBMS_XMLGEN.getXML(Ctx);
    --
    IF temp_xml IS NOT NULL THEN
        IF xml IS NOT NULL THEN
            DBMS_LOB.APPEND( xml, temp_xml );
        ELSE
            xml := temp_xml;
        END IF;
    END IF;
    --
    DBMS_XMLGEN.closeContext( Ctx );
    
    v_file := utl_file.fopen (my_dir, filename, 'A');
    utl_file.fclose(v_file);
END;

