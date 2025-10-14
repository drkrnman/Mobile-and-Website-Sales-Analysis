

--- age group for specified date
CREATE FUNCTION dbo.fn_AgeCategory (@birth_dt DATE, @current_dt DATE)
RETURNS NVARCHAR(50)
AS
BEGIN
    DECLARE @category NVARCHAR(50);
	DECLARE @age INT;
	
	SET @age = DATEDIFF(YEAR, @birth_dt, @current_dt);

    IF      @age < 14  SET @category = '1: less than 14';
    ELSE IF @age < 18  SET @category = '2: 14 – 17';
    ELSE IF @age < 24  SET @category = '3: 18 – 23';
    ELSE IF @age < 28  SET @category = '4: 24 – 27';
    ELSE IF @age < 35  SET @category = '5: 28 – 34';
    ELSE IF @age < 50  SET @category = '6: 35 – 49';
    ELSE IF @age < 66  SET @category = '7: 50 – 65';
    ELSE IF @age < 99  SET @category = '8: more than 65';
    ELSE               SET @category = '9: Not set';

    RETURN @category;
END;

--- category for specified amount
create function dbo.fn_AmountCategory(@amount numeric(18,2))
returns nvarchar(255)
as
begin
	declare @amount_category nvarchar(255);
	
	IF      @amount < 0          SET @amount_category = '0: Not set';
    ELSE IF @amount < 200000     SET @amount_category = '1: less than 200,000';
	ELSE IF @amount < 300000     SET @amount_category = '2: 200,000 - 300,000';
	ELSE IF @amount < 500000     SET @amount_category = '3: 300,000 - 500,000';
	ELSE IF @amount < 2000000    SET @amount_category = '4: 500,000 - 2,000,000';
	ELSE IF @amount < 100000000  SET @amount_category = '5: more than 2,000,000';
	ELSE                         SET @amount_category = '6: more than 100,000,000';
	
	return @amount_category
end;

