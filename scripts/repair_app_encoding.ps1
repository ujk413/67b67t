param(
  [Parameter(Mandatory = $true)]
  [string]$Path
)

$enc1251 = [System.Text.Encoding]::GetEncoding(1251)

function Convert-Mojibake1251ToUtf8([string]$text) {
  return [System.Text.Encoding]::UTF8.GetString($enc1251.GetBytes($text))
}

function Convert-ShiftedCp1251([string]$text) {
  $chars = foreach ($ch in $text.ToCharArray()) {
    $code = [int][char]$ch
    if ($code -ge 0x10 -and $code -le 0x4F) {
      [char]($code + 0xB0)
    } else {
      $ch
    }
  }
  return (-join $chars)
}

function Score-Text([string]$text) {
  $good = 0
  $badMojibake = 0
  $control = 0
  $shifted = 0
  $chars = $text.ToCharArray()

  for ($i = 0; $i -lt $chars.Length; $i++) {
    $code = [int][char]$chars[$i]

    if ($code -ge 0x0400 -and $code -le 0x04FF) {
      $good++
    }
    if (($code -ge 0x00 -and $code -le 0x08) -or ($code -ge 0x0B -and $code -le 0x0C) -or ($code -ge 0x0E -and $code -le 0x1F)) {
      $control++
    }
    if ($code -ge 0x10 -and $code -le 0x4F) {
      $shifted++
    }
    if ($code -eq 0xFFFD) {
      $badMojibake += 2
    }

    if ($i -lt ($chars.Length - 1)) {
      $next = [int][char]$chars[$i + 1]
      if (($code -eq 0x0420 -or $code -eq 0x0421 -or $code -eq 0x0440) -and ($next -ge 0x0400 -and $next -le 0x04FF)) {
        $badMojibake++
      }
    }
  }

  $questionRuns = ([regex]::Matches($text, '\?{4,}')).Count
  return ($good * 3) - ($badMojibake * 6) - ($control * 8) - ($shifted * 2) - ($questionRuns * 4)
}

$original = [System.IO.File]::ReadAllText($Path, [System.Text.Encoding]::UTF8)
$backupPath = "$Path.encoding.bak"
[System.IO.File]::WriteAllText($backupPath, $original, [System.Text.Encoding]::UTF8)

$lines = $original -split "`r?`n", -1
$fixedLines = foreach ($line in $lines) {
  $candidates = [System.Collections.Generic.List[string]]::new()
  $candidates.Add($line)
  $candidates.Add((Convert-Mojibake1251ToUtf8 $line))
  $candidates.Add((Convert-ShiftedCp1251 $line))
  $candidates.Add((Convert-ShiftedCp1251 (Convert-Mojibake1251ToUtf8 $line)))
  $candidates.Add((Convert-Mojibake1251ToUtf8 (Convert-ShiftedCp1251 $line)))

  $best = $line
  $bestScore = Score-Text $line

  foreach ($candidate in ($candidates | Select-Object -Unique)) {
    $score = Score-Text $candidate
    if ($score -gt $bestScore) {
      $best = $candidate
      $bestScore = $score
    }
  }

  $best
}

$fixed = [string]::Join("`r`n", $fixedLines)
[System.IO.File]::WriteAllText($Path, $fixed, [System.Text.Encoding]::UTF8)
