import { expect, test } from 'claude-code/testing'

test('напис видно на всіх поверхнях', async $ => {
  for (const surface of ['terminal', 'desktop', 'vscode', 'mobile'] as const) {
    const ui = await $.ui.mount({ plugin: 'love-band', surface, component: 'AbovePrompt', props: {} })
    expect(await ui.find({ type: 'Text', text: /я люблю тебе/ })).toBeDefined()
    await ui.unmount()
  }
})

test('панель з написом малюється на всіх поверхнях', async $ => {
  for (const surface of ['terminal', 'desktop', 'vscode', 'mobile'] as const) {
    const ui = await $.ui.mount({ plugin: 'love-band', surface, component: 'Pane', requestId: 'love', props: {} })
    expect(await ui.find({ type: 'Text', text: /я люблю тебе/ })).toBeDefined()
    await ui.unmount()
  }
})
