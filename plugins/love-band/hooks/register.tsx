import type { Register } from 'claude-code'

const PANE = 'love'
const TEXT = '♥ ♡ ♥  я люблю тебе  ♥ ♡ ♥'

export const register: Register = on => {
  on('session.start', async ($, e, next) => {
    $.ui.toast('💖 я люблю тебе 💖')
    await $.command.register({ name: 'love', description: 'Показати «я люблю тебе»' })
    void $.ui.open({ id: PANE, title: '💖' })

    return next(e)
  })

  on('session.attach', async ($, e, next) => {
    void $.ui.open({ id: PANE, title: '💖' })

    return next(e)
  })

  on('command.run', { command: 'love' }, async $ => {
    await $.ui.open({ id: PANE, title: '💖' })

    return { text: '💖 я люблю тебе 💖' }
  })

  on('ui.render', { component: 'Pane', requestId: PANE }, ($, e) => {
    const { Box, Text } = $.ui.resolve(e)

    return (
      <Box justifyContent="center" paddingX={1}>
        <Text bold color="#ff5fa2">{TEXT}</Text>
      </Box>
    )
  })

  on('ui.render', { component: 'AbovePrompt' }, ($, e, next) => {
    if (e.props.hasSurvey) {
      return next(e)
    }

    const { Box, Text } = $.ui.resolve(e)

    return (
      <Box
        key="love"
        borderStyle="round"
        borderColor="#ff5fa2"
        paddingX={2}
        justifyContent="center"
        hover={{ borderColor: '#ff1f7a' }}
      >
        <Text bold color="#ff5fa2" hover={{ color: '#ff1f7a' }}>
          {TEXT}
        </Text>
      </Box>
    )
  })
}
