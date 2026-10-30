# The GIF for the README, recorded with vhs (https://github.com/charmbracelet/vhs)
# against a cluster already up:
#
#     ./demo.sh up
#     vhs demo.tape        # writes demo.gif next to this file
#
# The watcher's restart and first seconds are left out; the rest plays at
# twice the speed it ran at.

Output demo.gif

Set Shell "bash"
Set FontSize 15
Set Width 1100
Set Height 720
Set Padding 16
Set TypingSpeed 40ms
Set PlaybackSpeed 2.0

Hide
Type "clear && ./demo.sh scale"
Enter
Sleep 14s
Show

Sleep 70s
