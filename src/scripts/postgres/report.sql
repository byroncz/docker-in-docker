SELECT 
    p.number AS policy_number,
    p.start_date,
    p.end_date,
    p.type AS policy_type,
    p.insurance_company,
    i.first_name AS insured_first_name,
    i.last_name AS insured_last_name,
    a.first_name AS agent_first_name,
    a.last_name AS agent_last_name,
    a.email AS agent_email,
    a.phone AS agent_phone,
    pr.premium_amount,
    pr.deductible_amount,
    pr.coverage_limit,
    pa.payment_status,
    pa.payment_date,
    pa.payment_amount,
    pa.payment_method,
    c.claim_status,
    c.claim_date,
    c.claim_amount,
    c.claim_description
FROM policy p
LEFT JOIN insured i ON p.number = i.policy_number AND p.type = i.policy_type
LEFT JOIN agents a ON p.number = a.policy_number AND p.type = a.policy_type
LEFT JOIN premium pr ON p.number = pr.policy_number AND p.type = pr.policy_type
LEFT JOIN payments pa ON p.number = pa.policy_number AND p.type = pa.policy_type
LEFT JOIN claims c ON p.number = c.policy_number AND p.type = c.policy_type
WHERE p.end_date > '2023-07-01';
