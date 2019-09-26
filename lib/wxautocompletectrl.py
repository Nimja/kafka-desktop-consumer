# -*- coding: utf-8 -*-
__license__ = """Copyright (c) 2008-2019, Toni RuÅ¾a, All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 'AS IS'
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE."""

import wx
import wx.html
from wx.lib.newevent import NewEvent

__author__ = "Toni RuÅ¾a <toni.ruza@gmail.com>"
__url__ = "http://bitbucket.org/raz/wxautocompletectrl"

(ValueChangedEvent, EVT_VALUE_CHANGED) = NewEvent()

def list_completer(a_list):
    template = "%s<b>%s</b>%s"
    def completer(query):
        formatted, unformatted = list(), list()
        if query:
            unformatted = [item for item in a_list if query in item]
            for item in unformatted:
                s = item.find(query)
                formatted.append(
                    template % (item[:s], query, item[s + len(query):])
                )

        return formatted, unformatted
    return completer

# noinspection PyPep8Naming
class SuggestionsPopup(wx.Frame):
    def __init__(self, parent):
        wx.Frame.__init__(
            self, parent,
            style=(wx.FRAME_NO_TASKBAR |
                   wx.FRAME_FLOAT_ON_PARENT |
                   wx.STAY_ON_TOP)
        )
        self.suggestions = self._ListBox(self)
        self.suggestions.SetItemCount(0)
        self.unformated_suggestions = None

        self.Sizer = wx.BoxSizer()
        self.Sizer.Add(self.suggestions, 1, wx.EXPAND)

    class _ListBox(wx.html.HtmlListBox):
        items = None

        def OnGetItem(self, n):
            return self.items[n]

    def SetSuggestions(self, suggestions, unformated_suggestions):
        self.suggestions.items = suggestions
        self.suggestions.SetItemCount(len(suggestions))
        self.suggestions.SetSelection(0)
        self.suggestions.Refresh()
        self.unformated_suggestions = unformated_suggestions

    def CursorUp(self):
        selection = self.suggestions.GetSelection()
        if selection > 0:
            self.suggestions.SetSelection(selection - 1)

    def CursorDown(self):
        selection = self.suggestions.GetSelection()
        last = self.suggestions.GetItemCount() - 1
        if selection < last:
            self.suggestions.SetSelection(selection + 1)

    def CursorHome(self):
        if self.IsShown():
            self.suggestions.SetSelection(0)

    def CursorEnd(self):
        if self.IsShown():
            self.suggestions.SetSelection(self.suggestions.GetItemCount() - 1)

    def GetSelectedSuggestion(self):
        return self.unformated_suggestions[self.suggestions.GetSelection()]

    def GetSuggestion(self, n):
        return self.unformated_suggestions[n]


# noinspection PyPep8Naming
class AutocompleteTextCtrl(wx.TextCtrl):

    completer = None
    popup = None
    onchange = None

    def __init__(self, parent, id_=wx.ID_ANY, value=wx.EmptyString,
                 pos=wx.DefaultPosition, size=wx.DefaultSize, style=0,
                 validator=wx.DefaultValidator, name=wx.TextCtrlNameStr,
                 height=300, completer=None, multiline=False, frequency=250,
                 append_mode=False):
        style = style | wx.TE_PROCESS_ENTER
        if multiline:
            style = style | wx.TE_MULTILINE
        wx.TextCtrl.__init__(
            self, parent, id_, value, pos, size, style, validator, name
        )
        self.height = height
        self.frequency = frequency
        self.append_mode = append_mode
        if completer:
            self.SetCompleter(completer)

        self.queued_popup = False
        self.skip_event = False

    def SetAppendMode(self, append_mode):
        self.append_mode = append_mode

    def GetAppendMode(self):
        return self.append_mode

    def SetCompleter(self, completer):
        """Initializes the autocompletion.

        The 'completer' has to be a function with one argument
        (the current value of the control, ie. the query)
        and it has to return two lists: formated (html) and unformated
        suggestions.
        """
        self.completer = completer

        frame = self.TopLevelParent

        self.popup = SuggestionsPopup(self.TopLevelParent)

        frame.Bind(wx.EVT_MOVE, self.OnMove)
        self.Bind(wx.EVT_TEXT, self.OnTextUpdate)
        self.Bind(wx.EVT_SIZE, self.OnSizeChange)
        self.Bind(wx.EVT_KEY_DOWN, self.OnKeyDown)
        self.popup.suggestions.Bind(wx.EVT_LEFT_DOWN, self.OnSuggestionClicked)
        self.popup.suggestions.Bind(wx.EVT_KEY_DOWN, self.OnSuggestionKeyDown)

    def AdjustPopupPosition(self):
        self.popup.Move(self.ClientToScreen((0, self.Size.height)).Get())
        self.popup.Layout()
        self.popup.Refresh()

    def OnMove(self, event):
        self.AdjustPopupPosition()
        event.Skip()

    def OnTextUpdate(self, event):
        if self.skip_event:
            self.skip_event = False
        elif not self.queued_popup:
            wx.CallLater(self.frequency, self.AutoComplete)
            self.queued_popup = True
        event.Skip()

    def AutoComplete(self):
        self.queued_popup = False
        if self.Value != "":
            formated, unformated = self.completer(self.Value)
            if len(formated) > 0:
                self.popup.SetSuggestions(formated, unformated)
                self.AdjustPopupPosition()
                self.Unbind(wx.EVT_KILL_FOCUS)
                self.popup.ShowWithoutActivating()
                self.SetFocus()
                self.Bind(wx.EVT_KILL_FOCUS, self.OnKillFocus)
            else:
                self.popup.Hide()
        else:
            self.popup.Hide()

    def OnSizeChange(self, event):
        self.popup.Size = (self.Size[0], self.height)
        event.Skip()

    def OnKeyDown(self, event):
        key = event.GetKeyCode()

        if key == wx.WXK_UP:
            self.popup.CursorUp()
            return

        elif key == wx.WXK_DOWN:
            self.popup.CursorDown()
            return

        elif key in (wx.WXK_RETURN, wx.WXK_NUMPAD_ENTER) and self.popup.Shown:
            self.skip_event = True
            if self.append_mode:
                self.AppendValue(self.popup.GetSelectedSuggestion())
            else:
                self.SetValueWithChange(self.popup.GetSelectedSuggestion())
            self.SetInsertionPointEnd()
            self.popup.Hide()
            return

        elif key == wx.WXK_HOME:
            self.popup.CursorHome()

        elif key == wx.WXK_END:
            self.popup.CursorEnd()

        elif event.ControlDown() and chr(key).lower() == "a":
            self.SelectAll()

        elif key == wx.WXK_ESCAPE:
            self.popup.Hide()
            return

        event.Skip()

    def OnSuggestionClicked(self, event):
        self.skip_event = True
        n = self.popup.suggestions.VirtualHitTest(event.Position[1])
        self.SetInsertionPointEnd()
        wx.CallAfter(self.SetFocus)
        self.SetValueWithChange(self.popup.GetSuggestion(n))
        event.Skip()

    def OnSuggestionKeyDown(self, event):
        key = event.GetKeyCode()
        if key in (wx.WXK_RETURN, wx.WXK_NUMPAD_ENTER):
            self.skip_event = True
            if self.append_mode:
                self.AppendValue(self.popup.GetSelectedSuggestion())
            else:
                self.SetValueWithChange(self.popup.GetSelectedSuggestion())
            self.SetInsertionPointEnd()
            self.popup.Hide()
        event.Skip()

    def OnKillFocus(self, event):
        if not self.popup.IsActive():
            self.popup.Hide()
        event.Skip()

    def AppendValue(self, selection_suggestion):
        value = self.GetValue().lower()
        selection = selection_suggestion.lower()
        maxpos = 0
        for i in range(1, len(value) + 1):
            send = value[-i:]
            if selection.startswith(send):
                maxpos = i
        self.SetValueWithChange(
            self.GetValue()[:len(value) - maxpos] + selection_suggestion
        )

    def SetValueWithChange(self, value):
        self.SetValue(value)
        self.popup.Hide()
        # Run on-change if present.
        if self.onchange:
            self.onchange()
