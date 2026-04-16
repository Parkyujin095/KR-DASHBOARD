@echo off
chcp 65001 > nul
setlocal EnableDelayedExpansion

:: bat 파일이 위치한 폴더를 기준으로 경로 자동 설정
set BASE_DIR=%~dp0

:: ============================================================
::  설정 영역 — 필요 시 수정
:: ============================================================

:: xlsm 파일 경로 (bat과 같은 폴더에 위치)
set XLSM_1=%BASE_DIR%sales_dashboard.xlsm

:: 매크로: ThisWorkbook 모듈의 AutoRefreshAndClose
:: (매크로 내부에서 Save + Application.Quit 처리함)
set MACRO_1=sales_dashboard.xlsm!AutoRefreshAndClose

:: Python 실행 파일
set PYTHON=python

:: 대시보드 스크립트 (bat과 같은 폴더)
set DASHBOARD=%BASE_DIR%sales_dashboard.py

:: 로그 폴더
set LOG_DIR=%BASE_DIR%logs
set LOG_FILE=%LOG_DIR%\run_%date:~0,4%%date:~5,2%%date:~8,2%.log

:: ============================================================
::  로그 폴더 생성
:: ============================================================
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo ============================================================ >> "%LOG_FILE%"
echo [%date% %time%] 자동 실행 시작 >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

:: ============================================================
::  STEP 1: Excel 매크로 실행 (PowerShell COM 자동화)
::  ※ AutoRefreshAndClose 가 내부에서 Save + Application.Quit 처리
:: ============================================================
echo [%time%] STEP 1: Excel 매크로 실행 중... >> "%LOG_FILE%"

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference = 'Stop';" ^
  "try {" ^
  "  $xl = New-Object -ComObject Excel.Application;" ^
  "  $xl.Visible = $false;" ^
  "  $xl.DisplayAlerts = $false;" ^
  "  $wb = $xl.Workbooks.Open('%XLSM_1%');" ^
  "  $xl.Run('%MACRO_1%');" ^
  "  Write-Host 'Excel 매크로 완료 (매크로 내부에서 종료됨)';" ^
  "} catch {" ^
  "  Write-Host ('Excel 오류: ' + $_.Exception.Message);" ^
  "  try { $xl.Quit() } catch {};" ^
  "  exit 1;" ^
  "}" >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% neq 0 (
    echo [%time%] [ERROR] Excel 매크로 실패 — 대시보드 실행 중단 >> "%LOG_FILE%"
    goto :end
)
echo [%time%] Excel 매크로 완료 >> "%LOG_FILE%"

:: Excel 프로세스가 완전히 종료될 때까지 대기 (매크로 내 10분 Wait 고려)
timeout /t 5 /nobreak > nul

:: ============================================================
::  STEP 2: Python 대시보드 실행
:: ============================================================
echo [%time%] STEP 2: 대시보드 실행 중... >> "%LOG_FILE%"

%PYTHON% "%DASHBOARD%" >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% neq 0 (
    echo [%time%] [ERROR] 대시보드 실행 실패 (exit code: %ERRORLEVEL%) >> "%LOG_FILE%"
    goto :end
)
echo [%time%] 대시보드 실행 완료 >> "%LOG_FILE%"

:: ============================================================
:end
echo [%date% %time%] 전체 완료 >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"
endlocal
