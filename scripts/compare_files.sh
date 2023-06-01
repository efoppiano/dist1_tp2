make client-logs-live > logs/client_logs_fault_tolerant_with_kills.$1.log
cat logs/client_logs_fault_tolerant_with_kills.$1.log | grep -oE "action.{0,}" > logs/client_logs_fault_tolerant_with_kills.$1.log.trimmed
cat logs/client_logs_fault_tolerant_with_kills.$1.log.trimmed client_logs_baseline.log | sort | uniq -u