-- 合成临床试验数据：3 个试验、6 万患者、30 万条检验记录
CREATE TABLE patients (
    patient_id bigint PRIMARY KEY,
    trial_id   text NOT NULL
);
CREATE INDEX patients_trial_idx ON patients (trial_id);

CREATE TABLE labs (
    lab_id     bigint PRIMARY KEY,
    patient_id bigint NOT NULL REFERENCES patients (patient_id),
    value      numeric(10,2) NOT NULL
);
CREATE INDEX labs_patient_idx ON labs (patient_id);

INSERT INTO patients
SELECT g, 'T-' || (100 + (g % 3) * 100)
FROM generate_series(1, 60000) AS g;

INSERT INTO labs
SELECT g * 10 + k, g, round((random() * 200 - 50)::numeric, 2)
FROM generate_series(1, 60000) AS g,
     generate_series(0, 4) AS k;

ANALYZE patients;
ANALYZE labs;
