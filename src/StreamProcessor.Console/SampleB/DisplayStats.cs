using Spectre.Console;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace StreamProcessor.ConsoleScr.SampleB
{
    public class DisplayStats
    {
        private Table _table;
        //private Action<LiveDisplayContext> lvdContextAction;
        //private LiveDisplay liveDisplay;
        private LiveDisplayContext _liveDisplayContext;
        private List<Stats> _stats;


        public DisplayStats(List<Stats> Stats)
        {
            _stats = Stats;

            _table = new Table().Centered();

            BuildColumns();
            AddRows();
        }



        private void BuildColumns()
        {
            // Columns
            _table.ShowHeaders = true;
            _table.AddColumn("Title");
            _table.Columns[0].PadRight(6);
            foreach (Stats stat in _stats)
            {
                _table.AddColumn(stat.Name);
            }
        }

        private void AddRows()
        {
            _table.AddRow(MarkUpValue("Msg Sent", "green"));
            _table.AddRow(MarkUpValue("Created Msg", "green"));
            _table.AddRow(MarkUpValue("Success Msg", "green"));
            _table.AddRow(MarkUpValue("Failure Msg", "green"));
            _table.AddRow(MarkUpValue("CB Tripped", "green"));
            _table.AddRow(MarkUpValue("Consumed Msg", "green"));

        }

        private void AddColumnsForStats()
        {
            int col = 0;
            foreach (Stats stat in _stats)
            {
                col++;
                _table.UpdateCell(0, col, MarkUpValue(stat.SuccessMessages.ToString(), "green"));
                _table.UpdateCell(1, col, MarkUpValue(stat.CreatedMessages.ToString(), "green"));
                _table.UpdateCell(2, col, MarkUpValue(stat.SuccessMessages.ToString(), "green"));
                _table.UpdateCell(3, col, MarkUpValue(stat.FailureMessages.ToString(), "red"));
                _table.UpdateCell(4, col, MarkUpValue(stat.CircuitBreakerTripped.ToString(), "red"));

            }
        }

        private string MarkUpValue(string value, string colorName, bool bold = false, bool underline = false,
            bool italic = false)
        {
            string val = "[" + colorName + "]";
            if (bold) val += "[bold]";


            val += value + "[/]";
            return val;
        }

        private void SaveContext(LiveDisplayContext liveDisplayContext)
        {

            _liveDisplayContext = liveDisplayContext;
            _liveDisplayContext.Refresh();
        }

        public void Refresh()
        {
            System.Console.Clear();
            AddColumnsForStats();
            AnsiConsole.Write(_table);
            //    _liveDisplayContext.Refresh();
        }
    }
}
