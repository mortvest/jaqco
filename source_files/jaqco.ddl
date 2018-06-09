CREATE TABLE test_relation(account_id LONG,
                           user_name CHAR(255),
                           balance LONG)
                           COMMENT 'KEY = (account_id)';
CREATE TABLE other_table(account_id LONG,
                         value INT)
                         COMMENT 'KEY = (account_id)';
