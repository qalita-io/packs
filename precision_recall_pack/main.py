from qalita_core.pack import Pack

pack = Pack()
pack.load_data("source")

data = pack.df_source

############################ Metrics

############################ Recommendations


pack.metrics.save()
pack.recommendations.save()

######################## Export:
