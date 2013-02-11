module util;

private import std.c.string;

string fromStringz(char* s)
{
        return cast(string) (s ? s[0 .. strlen(s)] : null);
}

string fromStringz(char* s, ulong len)
{
        return cast(string) (s ? s[0 .. len] : null);
}

