CREATE TABLE test_relation(account_id BIGINT,
                           user_name CHAR(255),
                           balance DOUBLE PRECISION)
                           COMMENT 'KEY = (account_id)';
CREATE TABLE other_table(account_id BIGINT,
                         value INT);
                         // COMMENT 'KEY = (account_id)';
