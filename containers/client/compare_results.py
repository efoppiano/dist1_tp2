import logging

def compare_results(baseline: dict, results: dict) -> bool:
  
  error = False
  
  for city, queries in baseline.items(): 
    if city not in results:
      logging.warning(f"{city} not in results")
      continue
    
    for query, data in queries.items():
      if query not in results[city]:
        logging.error(f"{city} - {query} not in results")
        error = True
        continue

      for datum, values in data.items():
        if datum not in results[city][query]:
          logging.error(f"{city} - {query} - {datum} not in results")
          error = True
          continue

        for value in values:
          if value not in results[city][query][datum]:
            logging.error(f"{city} - {query} - {datum} - {value} not in results")
            error = True
            continue

          baseline_value = baseline[city][query][datum][value]
          results_value = results[city][query][datum][value]
          diff = abs(baseline_value - results_value)
          
          if diff >= 1:
            logging.error(f"{city} - {query} - {datum} - {value} - {baseline_value} vs {results_value}")
            error = True
          elif diff >= 0.05:
            logging.warning(f"{city} - {query} - {datum} - {value} - {baseline_value} vs {results_value}")
  
  return not error
            