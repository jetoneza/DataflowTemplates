CREATE TABLE
    company
(
    company_id      INT64 NOT NULL,
    company_name    STRING(100),
    created_on      STRING(100),    -- Converted created on to String
) PRIMARY KEY
  (company_id);
CREATE TABLE
    employee_sp                     -- Renamed employee to employee_sp
(
    employee_id         INT64 NOT NULL,
    company_id          INT64,
    employee_name       STRING(100),
    employee_address_sp STRING(100), -- Added employee_address to employee_address_sp
    created_on          DATE,
) PRIMARY KEY
  (employee_id);

CREATE TABLE
    employee_attribute
(
    employee_id    INT64 NOT NULL,
    attribute_name STRING(100) NOT NULL,
    value          STRING(100),
    updated_on     DATE,
) PRIMARY KEY
  (employee_id, attribute_name);
