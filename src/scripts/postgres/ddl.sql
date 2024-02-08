-- DROP TABLE IF EXISTS Payments;
-- DROP TABLE IF EXISTS Claims;
-- DROP TABLE IF EXISTS Insured;
-- DROP TABLE IF EXISTS Agents;
-- DROP TABLE IF EXISTS Premium;
-- DROP TABLE IF EXISTS Policy;

-- Creación de la tabla Policy
CREATE TABLE IF NOT EXISTS policy (
    number VARCHAR(255) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    type VARCHAR(255) NOT NULL,
    insurance_company VARCHAR(255) NOT NULL,
    PRIMARY KEY (number, type)
);

-- Creación de la tabla Insured
CREATE TABLE IF NOT EXISTS insured (
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    gender VARCHAR(50),
    age INT,
    address VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    postal_code VARCHAR(20),
    country VARCHAR(255),
    policy_number VARCHAR(255),
    policy_type VARCHAR(255) NOT NULL,
    FOREIGN KEY (policy_number, policy_type) REFERENCES policy(number, type)
);

-- Creación de la tabla Agents
CREATE TABLE IF NOT EXISTS agents (
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(20),
    policy_number VARCHAR(255),
    policy_type VARCHAR(255) NOT NULL,
    FOREIGN KEY (policy_number, policy_type) REFERENCES policy(number, type)
);      

-- Creación de la tabla Premium
CREATE TABLE IF NOT EXISTS premium (
    premium_amount DECIMAL(10, 2) NOT NULL,
    deductible_amount DECIMAL(10, 2) NOT NULL,
    coverage_limit DECIMAL(10, 2) NOT NULL,
    policy_number VARCHAR(255),
    policy_type VARCHAR(255) NOT NULL,
    FOREIGN KEY (policy_number, policy_type) REFERENCES policy(number, type)
);

-- Creación de la tabla Payments
CREATE TABLE IF NOT EXISTS payments (
    policy_number VARCHAR(255),
    payment_status VARCHAR(255) NOT NULL,
    payment_date DATE NOT NULL,
    payment_amount DECIMAL(10, 2) NOT NULL,
    payment_method VARCHAR(255) NOT NULL,
    policy_type VARCHAR(255) NOT NULL,
    FOREIGN KEY (policy_number, policy_type) REFERENCES policy(number, type)
);

-- Creación de la tabla Claims
CREATE TABLE IF NOT EXISTS claims (
    policy_number VARCHAR(255),
    claim_status VARCHAR(255) NOT NULL,
    claim_date DATE NOT NULL,
    claim_amount DECIMAL(10, 2) NOT NULL,
    claim_description TEXT,
    policy_type VARCHAR(255) NOT NULL,
    FOREIGN KEY (policy_number, policy_type) REFERENCES policy(number, type)
);