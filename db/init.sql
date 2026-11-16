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

-- ---------------------------------------------------------------------------
-- 差分隐私查询：每患者 ε 预算账本（跨试验累计，上限 1.0，不重置）
-- ---------------------------------------------------------------------------
CREATE TABLE privacy_ledger (
    patient_id bigint PRIMARY KEY REFERENCES patients (patient_id),
    spent      numeric(4,1) NOT NULL DEFAULT 0
               CHECK (spent >= 0 AND spent <= 1.0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- 全部患者预置 0 余额：扣费行始终存在，SELECT ... FOR UPDATE 可直接锁定，
-- 避免并发首插竞争；CHECK 约束作为不超支的最终防线。
INSERT INTO privacy_ledger (patient_id) SELECT patient_id FROM patients;

-- ---------------------------------------------------------------------------
-- 差分隐私查询：全局查询号 -> 一次性结果（幂等）
-- 只保存带噪结果与参数，绝不保存患者明细或真实计数。
-- ---------------------------------------------------------------------------
CREATE TABLE queries (
    query_id          text PRIMARY KEY,
    trial_id          text NOT NULL,
    epsilon           numeric(2,1) NOT NULL CHECK (epsilon IN (0.1, 0.2, 0.5)),
    status            text NOT NULL CHECK (status IN ('completed', 'rejected')),
    bucket_negative   double precision,
    bucket_zero_to_100 double precision,
    bucket_over_100   double precision,
    reason            text,
    created_at        timestamptz NOT NULL DEFAULT now()
);

ANALYZE patients;
ANALYZE labs;
ANALYZE privacy_ledger;
