-- Drop 'supplier_ids' column from 'sonar_runs' table
ALTER TABLE sonar_runs DROP COLUMN IF EXISTS supplier_ids;

-- Insert missing suppliers into 'suppliers' table as temporary suppliers
INSERT INTO suppliers (_id, name)
SELECT DISTINCT supplier_id, 'Temporary Supplier' 
FROM sonar_runs_suppliers
WHERE supplier_id NOT IN (SELECT _id FROM suppliers);

-- Add foreign key constraints to 'sonar_runs_suppliers' table
ALTER TABLE sonar_runs_suppliers
ADD CONSTRAINT fk_sonar_run
    FOREIGN KEY (sonar_run_id) REFERENCES sonar_runs(_id),
ADD CONSTRAINT fk_supplier
    FOREIGN KEY (supplier_id) REFERENCES suppliers(_id);
