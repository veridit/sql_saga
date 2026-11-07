#!/usr/bin/env bash
# Shared static checks for already-sanitized migration SQL. Not a SQL parser.
# Keep executable DO statements, but remove literals and dormant routine bodies.
executable_migration_sql() {
    perl -0777 -ne '
        sub migration_statements {
            my ($s) = @_;
            my $out = ""; my $statement = "";
            while (length $s) {
                if ($s =~ s/\A--[^\n]*//) { $out .= " "; $statement .= " "; next }
                if ($s =~ s/\A\/\*.*?\*\///s) { $out .= " "; $statement .= " "; next }
                if ($s =~ s/\A\x27(?:[^\x27]|\x27\x27)*\x27//s) { $out .= "\x27\x27"; $statement .= "\x27\x27"; next }
                if ($s =~ s/\A(\$[A-Za-z_0-9]*\$)(.*?)\1//s) {
                    my ($tag, $body) = ($1, $2);
                    if ($statement =~ /\bDO\s*(?:LANGUAGE\s+\w+\s*)?$/i) {
                        $out .= $tag . migration_statements($body) . $tag;
                    } else {
                        $out .= "$tag$tag";
                    }
                    $statement .= "$tag$tag";
                    next;
                }
                $s =~ s/\A(.)//s; my $character = $1;
                $out .= $character; $statement .= $character;
                $statement = "" if $character eq ";";
            }
            return $out;
        }
        print migration_statements($_);' "$@"
}

# Return 0 for a cascading statement, 1 for no match, or grep's error status.
migration_uses_cascade() {
    local migration_sql=${1//$'\n'/ }
    LC_ALL=C grep -iE '(^|[^[:alnum:]_])(DROP|TRUNCATE)[^[:alnum:]_][^;]*[^[:alnum:]_]CASCADE([^[:alnum:]_]|$)' <<< "$migration_sql" >/dev/null
}

# Reporting callbacks belong to upgrade_test.sh. A scan error must not become OK.
check_migration_no_cascade() {
    if migration_uses_cascade "$1"; then
        fail 'direct update script uses CASCADE (never cascade into user objects)'
    else
        case $? in
            1) ok 'direct update script uses no CASCADE' ;;
            *) die 'cannot scan direct update script for CASCADE' ;;
        esac
    fi
}
