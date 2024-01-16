REM Windows batch script to run 1+ Python program/scripts, sequentially, within
REM their virtual environment. This can be called from Windows Task Scheduler.
        
set original_dir=%D:\python-project\etl\project\etl%

set venv_root_dir="D:\python-project\etl\project\etl"
        
cd %venv_root_dir%
        
call %venv_root_dir%\Scripts\activate.bat
        
python realtime_soda_15.py
        
call %venv_root_dir%\Scripts\deactivate.bat
        
cd %original_dir%
    
exit /B 1