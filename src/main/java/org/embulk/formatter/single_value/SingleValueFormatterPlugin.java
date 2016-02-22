package org.embulk.formatter.single_value;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import org.embulk.config.Config;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.FileOutput;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.util.LineEncoder;

import org.joda.time.DateTimeZone;
import org.msgpack.value.Value;

public class SingleValueFormatterPlugin
        implements FormatterPlugin
{
    public interface PluginTask
            extends Task, LineEncoder.EncoderTask, TimestampFormatter.Task
    {
        @Config("column_name")
        @ConfigDefault("null")
        public Optional<String> getColumnName();

        @Config("null_string")
        @ConfigDefault("\"\"")
        String getNullString();

        @Config("timezone")
        @ConfigDefault("\"UTC\"")
        public String getTimezone();

        @Config("timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%6N %z\"")
        public String getTimestampFormat();
    }

    @Override
    public void transaction(ConfigSource config, Schema schema,
            FormatterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        control.run(task.dump());
    }

    private int getInputColumnIndex(Optional<String> columnName, Schema inputSchema)
    {
        if (columnName.isPresent()) {
            return inputSchema.lookupColumn(columnName.get()).getIndex();
        }
        return 0; // default is the first column
    }

    private Schema getOutputSchema(int inputColumnIndex, Schema inputSchema)
    {
        Column outputColumn = inputSchema.getColumn(inputColumnIndex);
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        builder.add(outputColumn);
        return new Schema(builder.build());
    }

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
            final FileOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final LineEncoder encoder = new LineEncoder(output, task);
        final String nullString = task.getNullString();

        final int inputColumnIndex = getInputColumnIndex(task.getColumnName(), inputSchema);
        final Schema outputSchema = getOutputSchema(inputColumnIndex, inputSchema);
        final DateTimeZone timezone  = DateTimeZone.forID(task.getTimezone());
        final TimestampFormatter timestampFormatter =
            new TimestampFormatter(task.getJRuby(), task.getTimestampFormat(), timezone);

        // create a file
        encoder.nextFile();

        return new PageOutput() {
            private final PageReader pageReader = new PageReader(inputSchema);

            public void add(Page page)
            {
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    outputSchema.visitColumns(new ColumnVisitor() {
                        public void booleanColumn(Column column)
                        {
                            if (!pageReader.isNull(inputColumnIndex)) {
                                addValue(Boolean.toString(pageReader.getBoolean(inputColumnIndex)));
                            }
                            else {
                                addNullString();
                            }
                        }

                        public void longColumn(Column column)
                        {
                            if (!pageReader.isNull(inputColumnIndex)) {
                                addValue(Long.toString(pageReader.getLong(inputColumnIndex)));
                            }
                            else {
                                addNullString();
                            }
                        }

                        public void doubleColumn(Column column)
                        {
                            if (!pageReader.isNull(inputColumnIndex)) {
                                addValue(Double.toString(pageReader.getDouble(inputColumnIndex)));
                            }
                            else {
                                addNullString();
                            }
                        }

                        public void stringColumn(Column column)
                        {
                            if (!pageReader.isNull(inputColumnIndex)) {
                                addValue(pageReader.getString(inputColumnIndex));
                            }
                            else {
                                addNullString();
                            }
                        }

                        public void timestampColumn(Column column)
                        {
                            if (!pageReader.isNull(inputColumnIndex)) {
                                Timestamp value = pageReader.getTimestamp(inputColumnIndex);
                                addValue(timestampFormatter.format(value));
                            }
                            else {
                                addNullString();
                            }
                        }

                        public void jsonColumn(Column column)
                        {
                            if (!pageReader.isNull(inputColumnIndex)) {
                                Value value = pageReader.getJson(inputColumnIndex);
                                addValue(value.toJson());
                            }
                            else {
                                addNullString();
                            }
                        }

                        private void addValue(String v)
                        {
                            encoder.addText(v);
                        }

                        private void addNullString()
                        {
                            encoder.addText(nullString);
                        }
                    });
                    encoder.addNewLine();
                }
            }

            public void finish()
            {
                encoder.finish();
            }

            public void close()
            {
                encoder.close();
            }
        };
    }
}
