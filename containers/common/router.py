class Router:

  def __init__(queue_name: str, amount: int):
    self.queue_name = queue_name
    self.amount = int(amount)

  def route(hashing_key: str) -> str:
    if self.amount is None:
      return f"{self.queue_name}"

    queue_num = hash(hashing_key) % self.amount
    return f"{self.queue_name}_{queue_num}"

  def publish() -> str:
    return f"publish_{self.queue_name}"
