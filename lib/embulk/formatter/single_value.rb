Embulk::JavaPlugin.register_formatter(
  "single_value", "org.embulk.formatter.single_value.SingleValueFormatterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
