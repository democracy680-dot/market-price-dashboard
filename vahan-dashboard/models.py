from collections import namedtuple

# One row of registration data: month "YYYY-MM", category label, maker name, unit count.
SaleRow = namedtuple("SaleRow", ["month", "category", "maker", "units"])
