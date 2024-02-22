from qalita_core.pack import *
from qalita_core.utils import *

pack = Pack()
pack.load_data("source")

data = pack.df_source

############################ Metrics

############################ Recommendations


pack.metrics.save()
pack.recommendations.save()

######################## Export:
