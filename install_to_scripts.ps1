function Add-To-Env-Path {
    param(        [string]$Directory    )

    # Check if the directory is already in the PATH
    if ($env:Path -split ';' -notcontains $Directory) {
        # Append the directory to the PATH
        $env:Path += ";$Directory"

        # Update the PATH environment variable permanently for the current user
        [Environment]::SetEnvironmentVariable("Path", $env:Path, "User")
        Write-Host "Path $Directory is added to ENVIRONMENT VARIABLE:PATH."
    }
    else {
        Write-Host "Path $Directory is in ENVIRONMENT VARIABLE:PATH already."
    }
}

function Find-And-New-Dir {
    param([string]$Directory)
    if (Test-Path -Path $Directory) {
        Write-Host "Path $Directory exists already."
    }
    else {
        mkdir $Directory
        Write-Host "Path $Directory is created."
    }
}

function Copy-To-Dir {
    param([string]$Src, [string]$Dst)
    Copy-Item $Src $Dst
    Write-Host "copy $Src to $Dst ..."
}

$custom_py_dir = Read-Host -Prompt "Please input the full path for the directory, like E:\TMP."
Add-To-Env-Path $custom_py_dir
Find-And-New-Dir $custom_py_dir

Copy-To-Dir -src src/husfort/utility/get_datetime_fromtimestamp.py -dst $custom_py_dir
Copy-To-Dir -src src/husfort/utility/view_colors.py -dst $custom_py_dir
Copy-To-Dir -src src/husfort/utility/view_csv.py -dst $custom_py_dir
Copy-To-Dir -src src/husfort/utility/view_h5.py -dst $custom_py_dir
Copy-To-Dir -src src/husfort/utility/view_sql.py -dst $custom_py_dir
