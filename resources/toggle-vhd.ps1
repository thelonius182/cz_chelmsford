$container = "C:\u1\usb_shuttle\vault.hc"
$letter    = "K"
$vc        = "C:\Program Files\VeraCrypt\VeraCrypt.exe"

# If drive exists → dismount silently
if (Test-Path "$letter`:\" ) {
    & $vc /dismount $letter /silent /quit
    Write-Host "Vault dismounted."
    exit
}

# Ask for password securely
$secure = Read-Host "Enter vault password" -AsSecureString
$plain  = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
          )

# Mount silently
& $vc /volume $container /letter $letter `
      /password "$plain" `
      /silent /quit

Start-Sleep -Milliseconds 2000

if (Test-Path "$letter`:\" ) {
    Start-Process "$letter`:\"
    Write-Host "Vault mounted and opened."
} else {
    Write-Host "Mount failed."
}
